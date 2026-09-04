defmodule Libremarket.Compras do
  def comprar(producto_id, forma_entrega, medio_pago) do
    GenServer.call(Libremarket.Compras.Server, {:comprar, producto_id, forma_entrega, medio_pago})
  end
end

defmodule Libremarket.Compras.Server do
  use GenServer

  def start_link(opts \\ %{}) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_state), do: {:ok, %{}}

  @impl true
  def handle_call({:comprar, producto_id, forma_entrega, medio_pago}, _from, compras) do
    id_compra = System.unique_integer([:positive])

    resultado = procesar_compra(id_compra, producto_id, forma_entrega, medio_pago)

    {:reply, resultado, Map.put(compras, id_compra, resultado)}
  end

  defp procesar_compra(id_compra, producto_id, forma_entrega, medio_pago) do
    reserva = Libremarket.Ventas.reservar_producto(producto_id)

    case reserva do
      {:error, :sin_stock} ->
        {:error, :sin_stock}

      {:ok, _producto} ->
        continuar_compra(id_compra, producto_id, forma_entrega, medio_pago)
    end
  end

  defp continuar_compra(id_compra, producto_id, forma_entrega, medio_pago) do
    hay_infraccion = Libremarket.Infracciones.Server.detectar_infraccion(id_compra)

    if hay_infraccion do
      Libremarket.Ventas.liberar_producto(producto_id)
      {:error, :infraccion}
    else
      costo_envio =
        if forma_entrega == :correo do
          Libremarket.Envios.calcular_costo()
        else
          0
        end

      resultado_pago = Libremarket.Pagos.Server.pago(id_compra)

      if resultado_pago == :rechazado do
        Libremarket.Ventas.liberar_producto(producto_id)
        {:error, :pago_rechazado}
      else
        if forma_entrega == :correo do
          Libremarket.Envios.agendar_envio(id_compra)
        end

        {:ok,
         %{
           id_compra: id_compra,
           producto_id: producto_id,
           forma_entrega: forma_entrega,
           medio_pago: medio_pago,
           costo_envio: costo_envio
         }}
      end
    end
  end
end
