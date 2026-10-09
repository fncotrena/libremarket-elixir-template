defmodule Libremarket.Pagos do

  def pago() do
    case Enum.random(1..100) <= 70 do
      true -> :autorizado
      false -> :rechazado
    end
  end

end


defmodule Libremarket.Pagos.Server do

  use GenServer
  use AMQP
  require Logger

  alias Libremarket.Middleware

  @queue_name "pagos_cola"
  @cola_compras "compras_cola"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def pago(pid \\ __MODULE__, id_compra) do
    GenServer.call(pid, {:pago, id_compra})
  end

  def listar_pagos(pid \\ __MODULE__) do
    GenServer.call(pid, :listar_pagos)
  end

  @impl true
  def init(state) do
    {:ok, channel} = Producer.get_channel()
    # Si el canal se cae, el servidor se reinicia y vuelve a suscribirse
    Process.monitor(channel.pid)

    # Declarar la cola de mensajes
    Queue.declare(channel, @queue_name, durable: true)

    # Configurar el consumidor
    Basic.consume(channel, @queue_name, nil, no_ack: true)

    {:ok, %{canal: channel, pagos: state, reloj: Middleware.nuevo()}}
  end

  @impl true
  def handle_call({:pago, id_compra}, _from, state) do
    {resultado, state} = pagar(id_compra, state)
    {:reply, resultado, state}
  end

  @impl true
  def handle_call(:listar_pagos, _from, state) do
    {:reply, state.pagos, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    # Middleware: mezclar el reloj recibido con el propio y avanzar el propio
    {mensaje, reloj_recibido} = Middleware.separar(payload)
    reloj = state.reloj |> Middleware.mezclar(reloj_recibido) |> Middleware.incrementar(:pagos)
    state = %{state | reloj: reloj}

    case String.split(mensaje, ":") do
      ["pagar", id] ->
        {resultado, state} = pagar(String.to_integer(id), state)
        reloj = Producer.send_message(@cola_compras, "pago:#{id}:#{resultado}", state.reloj, :pagos)
        {:noreply, %{state | reloj: reloj}}

      _ ->
        Logger.warning("Pagos: mensaje ignorado #{inspect(payload)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, motivo}, state) do
    {:stop, {:canal_caido, motivo}, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp pagar(id_compra, %{pagos: pagos} = state) do
    resultado = Libremarket.Pagos.pago()
    {resultado, %{state | pagos: Map.put(pagos, id_compra, resultado)}}
  end
end
