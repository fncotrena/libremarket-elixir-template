defmodule Libremarket.Compras do

def comprar(producto_id, cantidad) do
  GenServer.call(__MODULE__, {:comprar, producto_id, cantidad})
end

end

defmodule Libremarket.Compras.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def comprar(pid \\ __MODULE__, producto_id, cantidad) do
    GenServer.call(pid, {:comprar, producto_id, cantidad})
  end

   @impl true
  def init(_state) do
    productos = %{
      1 => %{nombre: "Mouse", stock: 10, tipo: :periferico},
      2 => %{nombre: "Teclado", stock: 5, tipo: :periferico},
      3 => %{nombre: "Monitor", stock: 3, tipo: :monitor},
      4 => %{nombre: "Notebook", stock: 2, tipo: :computadora}
    }

    {:ok, productos}
  end

  @impl true
  def handle_call({:comprar, producto_id, cantidad}, _from, productos) do

    producto = productos[producto_id]

    if producto == nil do
      {:reply, {:error, "Producto inexistente"}, productos}
    else
      if producto.stock < cantidad do
        {:reply, {:error, "Stock insuficiente"}, productos}
      else

        pago = Libremarket.Pagos.Server.pago()

        case pago do
          :autorizado ->
            nuevo_stock = producto.stock - cantidad

            nuevo_producto = %{producto | stock: nuevo_stock}

            nuevos_productos =
              Map.put(productos, producto_id, nuevo_producto)

            {:reply, {:ok, "Compra realizada", nuevo_producto},
             nuevos_productos}

          :rechazado ->
            {:reply, {:error, "Pago rechazado"}, productos}
        end
      end
    end
  end
end
