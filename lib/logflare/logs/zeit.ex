defmodule Logflare.Logs.Zeit do
  def handle_batch(batch) when is_list(batch) do
    Enum.map(batch, fn x ->
      x =
        cond do
          is_nil(x["message"]) ->
            custom_message =
              "#{x["requestId"]} | #{x["source"]} | #{x["proxy"]["statusCode"]} | #{
                x["proxy"]["host"]
              } | #{x["proxy"]["path"]} | #{x["proxy"]["clientIp"]} | #{x["proxy"]["userAgent"]}"

            Map.put(x, "message", custom_message)

          true ->
            x
        end

      if x["proxy"]["userAgent"] do
        [ua] = x["proxy"]["userAgent"]
        Kernel.put_in(x["proxy"]["userAgent"], ua)
      else
        x
      end
    end)
  end
end
