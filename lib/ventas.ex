defmodule Libremarket.Ventas do
  def reservar_producto(producto_id, cantidad \\ 1) do
    GenServer.call(Libremarket.Ventas.Server, {:reservar, producto_id, cantidad})
  end

  def liberar_producto(producto_id, cantidad \\ 1) do
    GenServer.call(Libremarket.Ventas.Server, {:liberar, producto_id, cantidad})
  end
end

defmodule Libremarket.Ventas.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    productos =
      for id <- 1..10, into: %{} do
        {id, %{nombre: "Producto #{id}", stock: Enum.random(1..10)}}
      end

    {:ok, productos}
  end

  @impl true
  def handle_call({:reservar, producto_id, cantidad}, _from, productos) do
    case Map.fetch(productos, producto_id) do
      :error ->
        {:reply, {:error, :producto_inexistente}, productos}

      {:ok, producto} when producto.stock < cantidad ->
        {:reply, {:error, :sin_stock}, productos}

      {:ok, producto} ->
        nuevo = %{producto | stock: producto.stock - cantidad}
        {:reply, {:ok, nuevo}, Map.put(productos, producto_id, nuevo)}
    end
  end

  @impl true
  def handle_call({:liberar, producto_id, cantidad}, _from, productos) do
    case Map.fetch(productos, producto_id) do
      :error ->
        {:reply, {:error, :producto_inexistente}, productos}

      {:ok, producto} ->
        nuevo = %{producto | stock: producto.stock + cantidad}
        {:reply, :ok, Map.put(productos, producto_id, nuevo)}
    end
  end
end