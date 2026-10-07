defmodule Libremarket.Envios do
  def calcular_costo() do
    GenServer.call(Libremarket.Envios.Server, :calcular_costo)
  end

  def agendar_envio(id_compra, empresa \\ :correo_argentino) do
    GenServer.call(Libremarket.Envios.Server, {:agendar_envio, id_compra, empresa})
  end

  def listar_envios() do
    GenServer.call(Libremarket.Envios.Server, :listar_envios)
  end
end

defmodule Libremarket.Envios.Server do

  use GenServer
  use AMQP
  require Logger

  @queue_name "envios_cola"
  @cola_compras "compras_cola"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(state) do
    {:ok, channel} = Producer.get_channel()
    Process.monitor(channel.pid)

    Queue.declare(channel, @queue_name, durable: true)

    Basic.consume(channel, @queue_name, nil, no_ack: true)

    {:ok, %{canal: channel, envios: state}}
  end

  @impl true
  def handle_call(:calcular_costo, _from, state) do
    {:reply, calcular_costo(), state}
  end

  @impl true
  def handle_call({:agendar_envio, id_compra, empresa}, _from, state) do
    {envio, state} = agendar(id_compra, empresa, state)
    {:reply, envio, state}
  end

  @impl true
  def handle_call(:listar_envios, _from, state) do
    {:reply, state.envios, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    case String.split(payload, ":") do
      ["calcular_costo", id] ->
        Producer.send_message(@cola_compras, "costo:#{id}:#{calcular_costo()}")
        {:noreply, state}

      ["agendar", id] ->
        {_envio, state} = agendar(String.to_integer(id), :correo_argentino, state)
        Producer.send_message(@cola_compras, "envio:#{id}:agendado")
        {:noreply, state}

      _ ->
        Logger.warning("Envios: mensaje ignorado #{inspect(payload)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, motivo}, state) do
    {:stop, {:canal_caido, motivo}, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp calcular_costo(), do: Enum.random(500..2000)

  defp agendar(id_compra, empresa, %{envios: envios} = state) do
    envio = %{empresa: empresa, estado: :agendado}
    {envio, %{state | envios: Map.put(envios, id_compra, envio)}}
  end
end
