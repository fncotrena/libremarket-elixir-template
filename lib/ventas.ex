defmodule Libremarket.Ventas do
  def reservar_producto(producto_id, cantidad \\ 1) do
    GenServer.call(Libremarket.Ventas.Server, {:reservar, producto_id, cantidad})
  end

  def liberar_producto(producto_id, cantidad \\ 1) do
    GenServer.call(Libremarket.Ventas.Server, {:liberar, producto_id, cantidad})
  end

  def listar_productos() do
    GenServer.call(Libremarket.Ventas.Server, :listar_productos)
  end
end

defmodule Libremarket.Ventas.Server do

  use GenServer
  use AMQP
  require Logger

  alias Libremarket.Middleware

  @queue_name "ventas_cola"
  @cola_compras "compras_cola"

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    productos =
      for id <- 1..10, into: %{} do
        {id, %{nombre: "Producto #{id}", stock: Enum.random(1..10)}}
      end

    {:ok, channel} = Producer.get_channel()
    Process.monitor(channel.pid)

    Queue.declare(channel, @queue_name, durable: true)

    Basic.consume(channel, @queue_name, nil, no_ack: true)

    {:ok, %{productos: productos, canal: channel, reloj: Middleware.nuevo()}}
  end

  @impl true
  def handle_call({:reservar, producto_id, cantidad}, _from, state) do
    case reservar(state.productos, producto_id, cantidad) do
      {:ok, nuevo, productos} -> {:reply, {:ok, nuevo}, %{state | productos: productos}}
      {:error, motivo} -> {:reply, {:error, motivo}, state}
    end
  end

  @impl true
  def handle_call({:liberar, producto_id, cantidad}, _from, state) do
    case liberar(state.productos, producto_id, cantidad) do
      {:ok, productos} -> {:reply, :ok, %{state | productos: productos}}
      {:error, motivo} -> {:reply, {:error, motivo}, state}
    end
  end

  @impl true
  def handle_call(:listar_productos, _from, state) do
    {:reply, state.productos, state}
  end

  @impl true
  def handle_info({:basic_consume_ok, _meta}, state), do: {:noreply, state}

  @impl true
  def handle_info({:basic_deliver, payload, _meta}, state) do
    # Middleware: mezclar el reloj recibido con el propio y avanzar el propio
    {mensaje, reloj_recibido} = Middleware.separar(payload)
    reloj = state.reloj |> Middleware.mezclar(reloj_recibido) |> Middleware.incrementar(:ventas)
    state = %{state | reloj: reloj}

    case String.split(mensaje, ":") do
      ["reservar", id, producto_id_str] ->
        case reservar(state.productos, String.to_integer(producto_id_str), 1) do
          {:ok, _nuevo, productos} ->
            reloj = Producer.send_message(@cola_compras, "reserva:#{id}:ok", state.reloj, :ventas)
            {:noreply, %{state | productos: productos, reloj: reloj}}

          {:error, motivo} ->
            reloj = Producer.send_message(@cola_compras, "reserva:#{id}:#{motivo}", state.reloj, :ventas)
            {:noreply, %{state | reloj: reloj}}
        end

      ["liberar", _id, producto_id_str] ->
        case liberar(state.productos, String.to_integer(producto_id_str), 1) do
          {:ok, productos} -> {:noreply, %{state | productos: productos}}
          {:error, _} -> {:noreply, state}
        end

      _ ->
        Logger.warning("Ventas: mensaje ignorado #{inspect(payload)}")
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, _pid, motivo}, state) do
    {:stop, {:canal_caido, motivo}, state}
  end

  @impl true
  def handle_info(_msg, state), do: {:noreply, state}

  defp reservar(productos, producto_id, cantidad) do
    case Map.fetch(productos, producto_id) do
      :error ->
        {:error, :producto_inexistente}

      {:ok, producto} when producto.stock < cantidad ->
        {:error, :sin_stock}

      {:ok, producto} ->
        nuevo = %{producto | stock: producto.stock - cantidad}
        {:ok, nuevo, Map.put(productos, producto_id, nuevo)}
    end
  end

  defp liberar(productos, producto_id, cantidad) do
    case Map.fetch(productos, producto_id) do
      :error ->
        {:error, :producto_inexistente}

      {:ok, producto} ->
        {:ok, Map.put(productos, producto_id, %{producto | stock: producto.stock + cantidad})}
    end
  end
end
