defmodule Libremarket.Middleware do

  @servidores [:compras, :ventas, :infracciones, :pagos, :envios]

  def nuevo do
    for servidor <- @servidores, into: %{}, do: {servidor, 0}
  end

  def incrementar(reloj, servidor) do
    Map.update!(reloj, servidor, &(&1 + 1))
  end

  def mezclar(reloj, otro) do
    Map.merge(reloj, otro, fn _servidor, a, b -> max(a, b) end)
  end


  def adjuntar(mensaje, reloj) do
    reloj_texto =
      @servidores
      |> Enum.map_join(",", fn servidor -> "#{servidor}:#{Map.get(reloj, servidor, 0)}" end)

    "#{mensaje}|#{reloj_texto}"
  end


  def separar(payload) do
    case String.split(payload, "|", parts: 2) do
      [mensaje, reloj_texto] ->
        reloj =
          reloj_texto
          |> String.split(",")
          |> Enum.reduce(nuevo(), fn par, acc ->
            case String.split(par, ":") do
              [nombre, valor] ->
                Map.put(acc, String.to_existing_atom(nombre), String.to_integer(valor))

              _ ->
                acc
            end
          end)

        {mensaje, reloj}

      [mensaje] ->
        {mensaje, nuevo()}
    end
  end
end
