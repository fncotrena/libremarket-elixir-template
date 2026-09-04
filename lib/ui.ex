defmodule Libremarket.Ui do

  def comprar(producto_id, forma_entrega, medio_pago) do
    Libremarket.Compras.comprar(producto_id, forma_entrega, medio_pago)
  end

end
