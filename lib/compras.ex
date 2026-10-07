defmodule Libremarket.Compras do
  @timeout 30_000

  def comprar(producto_id, forma_entrega, medio_pago) do
    GenServer.call(
      Libremarket.Compras.Server,
      {:comprar, producto_id, forma_entrega, medio_pago},
      @timeout
    )
  end

  def listar_compras() do
    GenServer.call(Libremarket.Compras.Server, :listar_compras)
  end
end

defmodule Libremarket.Compras.Server do

  use GenServer
  use AMQP
  require Logger

  @queue_name "compras_cola"
  @cola_ventas "ventas_cola"
  @cola_infracciones "infracciones_cola"
  @cola_pagos "pagos_cola"
  @cola_envios "envios_cola"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_state) do
    {:ok, channel} = Producer.get_channel()
    Process.monitor(channel.pid)

    Queue.declare(channel, @queue_name, durable: true)
    Queue.purge(channel, @queue_name)

    Basic.consume(channel, @queue_name, nil, no_ack: true)

    {:ok, %{canal: channel, compras: %{}, pendientes: %{}}}
  end

  @impl true
  def handle_call({:comprar, producto_id, forma_entrega, medio_pago}, from, state) do
    id_compra = System.unique_integer([:positive])

    compra = %{
      producto_id: producto_id,
      forma_entrega: forma_entrega,
      medio_pago: medio_pago,
      costo_envio: 0,
      from: from
    }

    Producer.send_message(@cola_ventas, "reservar:#{id_compra}:#{producto_id}")

    {:noreply, put_in(state.pendientes[id_compra], compra)}
  end

  @impl true
  def handle_call(:listar_compras, _from, state) do
    {:reply, state.compras, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    with [tipo, id_str | datos] <- String.split(payload, ":"),
         {id, ""} <- Integer.parse(id_str),
         {:ok, compra} <- Map.fetch(state.pendientes, id) do
      {:noreply, avanzar(tipo, datos, id, compra, state)}
    else
      _ ->
        Logger.warning("Compras: mensaje ignorado #{inspect(payload)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, motivo}, state) do
    {:stop, {:canal_caido, motivo}, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}


  defp avanzar("reserva", ["ok"], id, _compra, state) do
    Producer.send_message(@cola_infracciones, "detectar:#{id}")
    state
  end

  defp avanzar("reserva", ["sin_stock"], id, _compra, state) do
    finalizar(id, {:error, :sin_stock}, state)
  end

  defp avanzar("reserva", ["producto_inexistente"], id, _compra, state) do
    finalizar(id, {:error, :producto_inexistente}, state)
  end

  defp avanzar("infraccion", ["true"], id, compra, state) do
    liberar(id, compra)
    finalizar(id, {:error, :infraccion}, state)
  end

  defp avanzar("infraccion", ["false"], id, %{forma_entrega: :correo}, state) do
    Producer.send_message(@cola_envios, "calcular_costo:#{id}")
    state
  end

  defp avanzar("infraccion", ["false"], id, _compra, state) do
    Producer.send_message(@cola_pagos, "pagar:#{id}")
    state
  end

  defp avanzar("costo", [monto], id, _compra, state) do
    Producer.send_message(@cola_pagos, "pagar:#{id}")
    put_in(state.pendientes[id].costo_envio, String.to_integer(monto))
  end

  defp avanzar("pago", ["rechazado"], id, compra, state) do
    liberar(id, compra)
    finalizar(id, {:error, :pago_rechazado}, state)
  end

  defp avanzar("pago", ["autorizado"], id, %{forma_entrega: :correo}, state) do
    Producer.send_message(@cola_envios, "agendar:#{id}")
    state
  end

  defp avanzar("pago", ["autorizado"], id, compra, state) do
    finalizar(id, {:ok, resumen(id, compra)}, state)
  end

  defp avanzar("envio", ["agendado"], id, compra, state) do
    finalizar(id, {:ok, resumen(id, compra)}, state)
  end

  defp avanzar(tipo, datos, id, _compra, state) do
    Logger.warning("Compras: respuesta inesperada #{tipo} #{inspect(datos)} para compra #{id}")
    state
  end


  defp liberar(id, compra) do
    Producer.send_message(@cola_ventas, "liberar:#{id}:#{compra.producto_id}")
  end

  defp finalizar(id, resultado, state) do
    {compra, pendientes} = Map.pop(state.pendientes, id)
    GenServer.reply(compra.from, resultado)
    IO.puts("Compra #{id}: #{inspect(resultado)}")
    %{state | pendientes: pendientes, compras: Map.put(state.compras, id, resultado)}
  end

  defp resumen(id, compra) do
    %{
      id_compra: id,
      producto_id: compra.producto_id,
      forma_entrega: compra.forma_entrega,
      medio_pago: compra.medio_pago,
      costo_envio: compra.costo_envio
    }
  end
end
