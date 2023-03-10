defmodule Bolt.Sips.ConnectionSupervisor do
  @moduledoc """

  """

  use DynamicSupervisor

  alias Bolt.Sips.Protocol
  alias Bolt.Sips

  require Logger
  # @type via :: {:via, Registry, any}
  @name __MODULE__

  def start_link(init_args) do
    DynamicSupervisor.start_link(__MODULE__, init_args, name: @name)
  end

  @impl true
  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  the resulting connection name i.e. "write@localhost:7687" will be used to spawn new
  DBConnection processes, and for finding available connections
  """
  @spec start_child(any, any, Keyword.t) :: {:ok, pid}
  def start_child(role, url, config) do
    prefix = Keyword.get(config, :prefix, :default)

    connection_name = "#{prefix}_#{role}@#{url}"

    role_config =
      config
      |> Keyword.put(:role, role)
      |> Keyword.put(:name, via_tuple(connection_name))

    # Force usage of the 'bolt' protocol for each connection
    role_config =
      if Keyword.get(role_config, :schema) == "neo4j" do
        role_config
        |> Keyword.put(:schema, "bolt")
        |> Keyword.put(:hostname, url)
        |> Keyword.put(:url, "bolt://" <> url)
      else
        role_config
      end

    spec = %{
      id: connection_name,
      start: {DBConnection, :start_link, [Protocol, role_config]},
      type: :worker,
      restart: :transient,
      shutdown: 500
    }

    with(
      {:error, :not_found} <- find_connection(connection_name),
      {:ok, pid} <- DynamicSupervisor.start_child(@name, spec)
    )
    do
      {:ok, pid}
    else
      {:ok, pid, _info} ->
        {:ok, pid}
      {:error, {:already_started, pid}} ->
        {:ok, pid}
      pid ->
        {:ok, pid}
    end
  end

  @spec find_connection(atom, String.t, atom) :: {:ok, pid} | {:error, :not_found}
  def find_connection(role, url, prefix) do
    find_connection("#{prefix}_#{role}@#{url}")
  end

  @spec find_connection(String.t) :: {:error, :not_found} | {:ok, pid}
  def find_connection(name) do
    case Registry.lookup(Sips.registry_name(), name) do
      [{pid, _}] ->
        {:ok, pid}
      _ ->
        Logger.error("[Bolt.Sips] error could not find connection. Name was [#{name}]")
        {:error, :not_found}
    end
  end

  @spec terminate_connection(atom, String.t, atom) :: {:error, :not_found} | {:ok, pid}
  def terminate_connection(role, url, prefix \\ :default) do
    case find_connection(role, url, prefix) do
      {:ok, pid} = conn ->
        Process.exit(pid, :normal)
        connections()
        conn
      err ->
        err
    end
  end

  def connections() do
    _connections()
    |> Enum.map(fn pid ->
      Sips.registry_name()
      |> Registry.keys(pid)
      |> List.first()
    end)
  end

  defp _connections() do
    __MODULE__
    |> DynamicSupervisor.which_children()
    |> Enum.filter(fn {_, pid, _type, modules} ->
      case {pid, modules} do
        {:restarting, _} -> false
        {_pid, _} -> true
      end
    end)
    |> Enum.map(fn {_, pid, _, _} ->
      pid
    end)
  end

  def via_tuple(name) do
    {:via, Registry, {Bolt.Sips.registry_name(), name}}
  end
end
