defmodule Bolt.Sips.Router do
  @moduledoc """
  This "driver" works in tandem with Neo4j's [Causal Clustering](https://neo4j.com/docs/operations-manual/current/clustering/>) feature by directing read and write behaviour to appropriate cluster members
  """
  use GenServer
  require Logger

  alias Bolt.Sips.Routing.RoutingTable
  alias Bolt.Sips.{Protocol, ConnectionSupervisor, LoadBalancer, Response, Error}

  defmodule State do
    @moduledoc """
    todo:
    this is work in progress and will be used for defining the state of the Router (Gen)Server
    """
    @type role :: atom

    @type t :: %__MODULE__{
            connections: %{
              role => %{String.t() => non_neg_integer},
              updated_at: non_neg_integer,
              ttl: non_neg_integer
            }
          }

    @enforce_keys [:connections]
    defstruct @enforce_keys
  end

  @no_routing nil
  @routing_table_keys [:read, :write, :route, :updated_at, :ttl, :error]


  # PUBLIC API

  @spec configure(Keyword.t) :: any
  def configure(opts) do
    GenServer.call(__MODULE__, {:configure, opts})
  end

  @spec get_connection(atom, atom) :: {:ok, DBConnection.conn()} | {:error, any}
  def get_connection(role, prefix \\ :direct) do
    GenServer.call(__MODULE__, {:get_connection, role, prefix})
  end

  def terminate_connections(role, prefix \\ :default) do
    GenServer.call(__MODULE__, {:terminate_connections, role, prefix})
  end

  def info() do
    GenServer.call(__MODULE__, :info)
  end

  def routing_table(prefix) do
    GenServer.call(__MODULE__, {:routing_table_info, prefix})
  end

  @spec start_link(Keyword.t()) :: :ignore | {:error, Keyword.t()} | {:ok, pid()}
  def start_link(init_args) do
    GenServer.start_link(__MODULE__, init_args, name: __MODULE__)
  end

  @impl true
  @spec init(Keyword.t()) :: {:ok, State.t(), {:continue, :post_init}}
  def init(options) do
    {:ok, options, {:continue, :post_init}}
  end


  # CALLBACKS

  @impl true
  def handle_call({:configure, opts}, _from, state) do
    prefix = Keyword.get(opts, :prefix, :default)
    %{connections: current_connections} = Map.get(state, prefix, %{connections: %{}})

    %{user_options: user_options, connections: connections} =
      try do
        opts
        |> _configure()
        |> Map.get(prefix)
      rescue
        e in Bolt.Sips.Exception ->
          %{user_options: opts, connections: %{error: e.message}}
      end

    updated_connections = Map.merge(current_connections, connections)

    new_state =
      Map.put(state, prefix, %{user_options: user_options, connections: updated_connections})

    {:reply, new_state, new_state}
  end

  # getting connections for role in [:route, :read, :write]
  @impl true
  def handle_call({:get_connection, role, prefix}, _from, state) when role in [:route, :read, :write] do
    with(
      %{connections: connections} <- Map.get(state, prefix),
      {:ok, conn, updated_connections} <- _get_connection(role, connections, prefix)
    )
    do
      {:reply, {:ok, conn}, put_in(state, [prefix, :connections], updated_connections)}
    else
      err ->
        Logger.error("[Bolt.Sips] error in get_connection callback for [:route, :read, :write] roles, [#{err}]")
        err_msg = error_no_connection_available_for_role(role, err, prefix)
        {:reply, {:error, err_msg}, state}
    end
  end

  # getting connections for any user defined roles, or: `:direct`
  @impl true
  def handle_call({:get_connection, role, prefix}, _from, state) do
    with(
      %{connections: connections} <- Map.get(state, prefix),
      true <- Map.has_key?(connections, role),
      [url | _none] <- connections |> Map.get(role) |> Map.keys(),
      {:ok, pid} <- ConnectionSupervisor.find_connection(role, url, prefix)
    )
    do
      {:reply, {:ok, pid}, state}
    else
      e ->
        err_msg = error_no_connection_available_for_role(role, e, prefix)
        {:reply, {:error, err_msg}, state}
    end
  end

  @impl true
  def handle_call({:terminate_connections, role, prefix}, _from, state) do
    %{connections: connections} = Map.get(state, prefix, %{})

    with true <- Map.has_key?(connections, role),
         :ok <-
           connections
           |> Map.get(role)
           |> Map.keys()
           |> Enum.each(&ConnectionSupervisor.terminate_connection(role, &1, prefix)) do
      new_connections = Map.delete(connections, role)
      new_state = put_in(state, [prefix, :connections], new_connections)
      {:reply, :ok, new_state}
    else
      _e ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:info, _from, state) do
    {:reply, state, state}
  end

  def handle_call({:routing_table_info, prefix}, _from, state) do
    routing_table =
      with connections when not is_nil(connections) <- get_in(state, [prefix, :connections]) do
        Map.take(connections, @routing_table_keys)
      end

    {:reply, routing_table, state}
  end

  @impl true
  @spec handle_continue(:post_init, Keyword.t()) :: {:noreply, map}
  def handle_continue(:post_init, opts) do
    {:noreply, _configure(opts)}
  end

  @impl true
  def handle_info({:refresh, prefix}, state) do
    %{connections: connections, user_options: user_options} = Map.get(state, prefix)

    %{ttl: ttl} = connections
    # may overwrite the ttl, when desired in exceptional situations: tests, for example.
    ttl = Keyword.get(user_options, :ttl, ttl)

    state =
      with(
        {:ok, routing_table, _updated_connections} <- get_routing_table(connections, true, prefix),
        {:ok, new_connections} <- start_connections(user_options, routing_table)
      )
      do
        connections =
          connections
          |> Map.put(:updated_at, Bolt.Sips.Utils.now())
          |> merge_connections_maps(new_connections, prefix)

        ttl = Keyword.get(user_options, :ttl, ttl * 1000)

        Process.send_after(self(), {:refresh, prefix}, ttl)

        new_state = %{user_options: user_options, connections: connections}
        Map.put(state, prefix, new_state)
      else
        err ->
          Logger.error("Cannot create any connections. Error: #{inspect(err)}")
          Map.put(state, prefix, %{user_options: user_options, connections: %{}})
      end

    {:noreply, state}
  end

  @impl true
  def handle_info(req, state) do
    Logger.warn("Router received an unexpected message: #{inspect(req)}")
    {:noreply, state}
  end


  # PRIVATE API

  defp _configure(opts) do
    options = Bolt.Sips.Utils.default_config(opts)

    prefix = Keyword.get(options, :prefix, :default)

    ssl_or_sock = if(Keyword.get(options, :ssl), do: :ssl, else: Keyword.get(options, :socket))

    user_options = Keyword.put(options, :socket, ssl_or_sock)
    with_routing? = Keyword.get(user_options, :schema, "bolt") =~ ~r/(^neo4j$)|(^bolt\+routing$)/i

    with(
      {:ok, routing_table} <- get_routing_table(user_options, with_routing?),
      {:ok, connections} <- start_connections(user_options, routing_table)
    )
    do
      connections = Map.put(connections, :routing_query, routing_table[:routing_query])

      %{prefix => %{user_options: user_options, connections: connections}}
    else
      {:error, msg} ->
        Logger.error("cannot load the routing table. Error: #{msg}")
        %{prefix => %{user_options: user_options, connections: %{error: "Not a router"}}}
    end
  end

  @spec get_routing_table(map, any, atom) :: {:ok, any, map} | {:error, String.t | :routing_table_not_available | :routing_table_not_available_at_all}
  defp get_routing_table(%{routing_query: %{params: props, query: query}} = connections, _, prefix) do
    with(
      {:ok, conn, updated_connections} <- _get_connection(:route, connections, prefix),
      {:ok, %Response{} = results} <- Bolt.Sips.query(conn, query, props)
    )
    do
      {:ok, Response.first(results), updated_connections}
    else
      {:error, %Error{code: _code, message: message}} ->
        Logger.error("[Bolt.Sips] error while getting routing table. In query, [#{message}]")
        {:error, message}

      {:error, msg, _updated_connections} ->
        Logger.error("[Bolt.Sips] error while getting routing table. Unable to get connection, [#{msg}]")
        {:error, :routing_table_not_available}

      err ->
        Logger.error("[Bolt.Sips] get_routing_table error: #{inspect(err)}")
        {:error, :routing_table_not_available_at_all}
    end
  end

  defp get_routing_table(_opts, false) do
    {:ok, @no_routing}
  end

  defp get_routing_table(opts, _) do
    prefix = Keyword.get(opts, :prefix, :default)

    with(
      {:ok, %Protocol.ConnData{configuration: configuration}} <- Protocol.connect(opts),
      # DON'T>  :ok <- Protocol.disconnect(:stop, conn),
      {_long, short} <- parse_server_version(configuration[:server_version])
    )
    do
      {query, params} =
        if Version.match?(short, ">= 3.2.3") do
          props = Keyword.get(opts, :routing_context, %{})
          {"CALL dbms.cluster.routing.getRoutingTable($context)", %{context: props}}
        else
          {"CALL dbms.cluster.routing.getServers()", %{}}
        end

      with(
        {:ok, pid} <- DBConnection.start_link(Protocol, Keyword.delete(opts, :name)),
        {:ok, %Response{} = results} <- Bolt.Sips.query(pid, query, params),
        true <- Process.exit(pid, :normal)
      )
      do
        table =
          results
          |> Response.first()
          |> Map.put(:routing_query, %{query: query, params: params})

        ttl = Map.get(table, :ttl, 300) * 1000
        # may overwrite the ttl, when desired in exceptional situations: tests, for example.
        ttl = Keyword.get(opts, :ttl, ttl)

        Process.send_after(self(), {:refresh, prefix}, ttl)

        {:ok, table}
      else
        {:error, %Error{message: message}} ->
          Logger.error(message)
          {:error, message}

        _e ->
          Logger.error("Are you sure you're connected to a Neo4j cluster? The routing table is not available.")

          {:error, :routing_table_not_available}
      end
    end
  end

  @doc """
  start a new (DB)Connection process, supervised registered under a name following this convention:

  - "role@hostname:port", the `role`, `hostname` and the `port` are collected from the user's
   configuration: `opts`. The `role` parameter is ignored when the `routing_table` parameter represents
   a neo4j map containing the definition for a neo4j cluster! It defaults to: `:direct`, when not specified!
  """
  @spec start_connections(any(), map | nil) :: {:ok, map}
  defp start_connections(opts, nil) do
    url = "#{opts[:hostname]}:#{opts[:port]}"
    role = Keyword.get(opts, :role, :direct)

    {:ok, _pid} = ConnectionSupervisor.start_child(role, url, opts)

    {:ok, %{role => %{url => 0}}}
  end

  defp start_connections(opts, raw_routing_table) do
    routing_table = RoutingTable.parse(raw_routing_table)

    connections =
      routing_table.roles
      |> Enum.reduce(%{}, fn {role, addresses}, acc ->
        addresses
        |> Enum.reduce(acc, fn {address, count}, acc ->
          # interim hack; force the schema to be `bolt`, otherwise the parse is not happening
          url = "bolt://" <> address
          %URI{host: host, port: port} = URI.parse(url)

          # Important!
          # We remove the url from the routing-specific configs, because the port and the address where the
          # socket will be opened, is using the host and the port returned by the routing table, and not by the
          # initial url param. The Utils will overwrite them if the `url` is defined!
          config =
            opts
            |> Keyword.put(:host, String.to_charlist(host))
            |> Keyword.put(:port, port)
            |> Keyword.put(:name, role)
            |> Keyword.put(:hits, count)
            |> Keyword.delete(:url)

          {:ok, _pid} = ConnectionSupervisor.start_child(role, address, config)

          Map.update(acc, role, %{address => 0}, fn urls -> Map.put(urls, address, 0) end)
        end)
        |> Map.merge(acc)
      end)
      |> Map.put(:ttl, routing_table.ttl)
      |> Map.put(:updated_at, routing_table.updated_at)

    {:ok, connections}
  end

  @doc """
  parse the version string received from the server, while considering the lack of the
  patch number in some situations

  ## Examples

  iex> Bolt.Sips.Router.parse_server_version(%{"server" => "Neo4j/3.5.0"})
  {"Neo4j/3.5.0", "3.5.0"}

  iex> Bolt.Sips.Router.parse_server_version(%{"server" => "Neo4j/3.5"})
  {"Neo4j/3.5", "3.5.0"}

  iex> Bolt.Sips.Router.parse_server_version(%{"server" => "Neo4j/3.5.10"})
  {"Neo4j/3.5.10", "3.5.10"}

  iex> Bolt.Sips.Router.parse_server_version(%{"server" => "Neo4j/3.5.11.1"})
  {"Neo4j/3.5.11.1", "3.5.11"}

  """
  @spec parse_server_version(map) :: {binary, <<_::16, _::_*8>>}
  def parse_server_version(%{"server" => _server_version_string}) do
    server_version_string = "Neo4j/4.4"

    server_version_regex = ~r/Neo4j\/(?<M>\d+)\.(?<m>\d+)\.(?<p>\d+)/

    %{"M" => major, "m" => minor, "p" => patch} = Regex.named_captures(server_version_regex, server_version_string <> ".0")

    {server_version_string, "#{major}.#{minor}.#{patch}"}
  end

  def parse_server_version(some_version) do
    raise ArgumentError, "not a Neo4J version: " <> inspect(some_version)
  end

  @spec error_no_connection_available_for_role(any(), any(), atom()) :: String.t
  defp error_no_connection_available_for_role(role, _e, prefix \\ :default) do
    "no connection exists with this role: #{role} (prefix: #{prefix})"
  end

  @routing_roles ~w{read write route}a
  @spec merge_connections_maps(any(), any(), atom()) :: any()
  def merge_connections_maps(current_connections, new_connections, prefix \\ :default) do
    @routing_roles
    |> Enum.flat_map(fn role ->
      new_urls = Map.keys(new_connections[role])

      current_connections[role]
      |> Map.keys()
      |> Enum.flat_map(fn url ->
        if url in new_urls do
          []
        else
          [{role, url}]
        end
      end)
    end)
    |> close_connections(prefix)

    @routing_roles
    |> Enum.reduce(current_connections, fn role, acc ->
      Map.put(acc, role, new_connections[role])
    end)
  end

  # [
  #   read: "localhost:7689",
  #   write: "localhost:7687",
  #   write: "localhost:7690",
  #   route: "localhost:7688",
  #   route: "localhost:7689"
  # ]
  @spec close_connections(any, any) :: :ok
  defp close_connections(connections, prefix) do
    Enum.each(connections, fn {role, url} ->
      case ConnectionSupervisor.terminate_connection(role, url, prefix) do
        {:ok, pid} ->
          {:ok, pid}
        {:error, :not_found} ->
          Logger.debug("#{role}: #{url}; not a valid connection/process. It can't be terminated")
      end
    end)
  end

  @spec _get_connection(String.t() | atom, map, atom) :: {:ok, pid, map} | {:error, any, map}
  defp _get_connection(role, connections, prefix) do
    with(
      true <- Map.has_key?(connections, role),
      {:ok, url} <- LoadBalancer.least_reused_url(Map.get(connections, role)),
      {:ok, pid} <- ConnectionSupervisor.find_connection(role, url, prefix)
    )
    do
      {_, updated_connections} = get_and_update_in(connections, [role, url], fn hits -> {hits, hits + 1} end)

      {:ok, pid, updated_connections}
    else
      err ->
        Logger.error("[Bolt.Sips] error in _get_connection [#{inspect(err)}]")
        err_msg = error_no_connection_available_for_role(role, err)
        {:error, err_msg, connections}
    end
  end
end
