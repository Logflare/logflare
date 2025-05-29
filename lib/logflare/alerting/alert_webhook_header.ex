defmodule Logflare.Alerting.AlertWebhookHeader do
  use Ecto.Schema
  import Ecto.Changeset

  @banned_headers [
    "content-type",
    "content-length",
    "transfer-encoding",
    "host",
    "connection",
    "accept-encoding",
    "user-agent"
  ]

  @primary_key false
  embedded_schema do
    field :key, :string
    field :value, :string
  end

  def changeset(header, attrs) do
    header
    |> cast(attrs, [:key, :value])
    |> update_change(:key, fn key ->
      key |> to_string() |> String.trim() |> String.downcase()
    end)
    |> update_change(:value, fn value ->
      value |> to_string() |> String.trim()
    end)
    |> validate_required([:key, :value])
    |> validate_change(:key, fn :key, key ->
      cond do
        key == "" ->
          [key: "can't be blank"]

        key in @banned_headers ->
          [key: "is a restricted header and cannot be set"]

        # Regex from RFC header key definition (no spaces or separators)
        not Regex.match?(~r/^[!#$%&'*+\-.^_`|~0-9a-z]+$/i, key) ->
          [key: "contains invalid characters"]

        true ->
          []
      end
    end)
    |> validate_change(:value, fn :value, value ->
      if value == "" do
        [value: "can't be blank"]
      else
        []
      end
    end)
  end
end
