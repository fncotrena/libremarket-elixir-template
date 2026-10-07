defmodule Libremarket.Infracciones do

  def detectar_infraccion() do
    Enum.random(1..100) <= 30
  end

end

defmodule Libremarket.Infracciones.Server do
 
  use GenServer
  use AMQP
  require Logger

  @queue_name "infracciones_cola"
  @cola_compras "compras_cola"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def detectar_infraccion(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:detectar_infraccion, id_compra})
  end

  def listar_infracciones(pid \\ __MODULE__) do
    GenServer.call(pid, :listar_infracciones)
  end

  @impl true
  def init(state) do
    {:ok, channel} = Producer.get_channel()
    Process.monitor(channel.pid)

    Queue.declare(channel, @queue_name, durable: true)

    Basic.consume(channel, @queue_name, nil, no_ack: true)

    {:ok, %{canal: channel, infracciones: state}}
  end

  @impl true
  def handle_call({:detectar_infraccion, id_compra}, _from, state) do
    {result, state} = detectar(id_compra, state)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:listar_infracciones, _from, %{infracciones: infracciones} = state) do
    {:reply, infracciones, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    case String.split(payload, ":") do
      ["detectar", id] ->
        {result, state} = detectar(String.to_integer(id), state)
        Producer.send_message(@cola_compras, "infraccion:#{id}:#{result}")
        {:noreply, state}

      _ ->
        Logger.warning("Infracciones: mensaje ignorado #{inspect(payload)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, motivo}, state) do
    {:stop, {:canal_caido, motivo}, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp detectar(id_compra, %{infracciones: infracciones} = state) do
    result = Libremarket.Infracciones.detectar_infraccion()
    {result, %{state | infracciones: Map.put(infracciones, id_compra, result)}}
  end
end
