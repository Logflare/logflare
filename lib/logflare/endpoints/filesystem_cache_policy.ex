defmodule Logflare.Endpoints.FilesystemCachePolicy do
  @moduledoc """
  Endpoint-owned time-window rules for passive ClickHouse filesystem-cache reads.
  Rules are versioned with the SQL whose parameters they describe.
  """

  import Ecto.Changeset

  alias Logflare.Sql

  @setting "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @formats ~w(iso8601 unix_seconds unix_milliseconds)
  @units %{"seconds" => 1, "minutes" => 60, "hours" => 3_600, "days" => 86_400}
  @keys ~w(start_param end_param timestamp_format bypass_fill_above_seconds enabled window_threshold window_unit)
  @max_threshold 9_223_372_036_854

  @type decision :: %{window_seconds: number() | nil, settings: map()}

  @spec normalize(term()) :: {:ok, map() | nil} | {:error, String.t()}
  def normalize(nil), do: {:ok, nil}
  def normalize(%{"enabled" => enabled}) when enabled in [false, "false"], do: {:ok, nil}

  def normalize(policy) when is_map(policy) do
    with true <- Enum.all?(Map.keys(policy), &(&1 in @keys)),
         %{"start_param" => start, "end_param" => finish, "timestamp_format" => format} <- policy,
         true <- valid_param?(start) and valid_param?(finish) and start != finish,
         true <- format in @formats,
         {:ok, threshold} <- threshold(policy) do
      {:ok,
       %{
         "start_param" => start,
         "end_param" => finish,
         "timestamp_format" => format,
         "bypass_fill_above_seconds" => threshold
       }}
    else
      _ ->
        {:error,
         "requires distinct start/end parameters, a timestamp format, and a positive threshold"}
    end
  end

  def normalize(_), do: {:error, "must be a time-window policy"}

  @spec validate(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  def validate(changeset) do
    case normalize(get_field(changeset, :filesystem_cache_policy)) do
      {:ok, nil} ->
        put_change(changeset, :filesystem_cache_policy, nil)

      {:ok, policy} ->
        validate_mapping(changeset, policy)

      {:error, message} ->
        add_error(changeset, :filesystem_cache_policy, message)
    end
  end

  @spec resolve(map() | nil, map()) :: {:ok, decision()} | {:error, String.t()}
  def resolve(nil, _params), do: {:ok, %{window_seconds: nil, settings: %{}}}

  def resolve(policy, params) do
    with {:ok, policy} when is_map(policy) <- normalize(policy),
         {:ok, start} <- timestamp(params[policy["start_param"]], policy["timestamp_format"]),
         {:ok, finish} <- timestamp(params[policy["end_param"]], policy["timestamp_format"]),
         true <- finish >= start do
      window = finish - start

      settings =
        if window > policy["bypass_fill_above_seconds"] * 1_000_000,
          do: %{@setting => 1},
          else: %{}

      {:ok, %{window_seconds: window / 1_000_000, settings: settings}}
    else
      _ ->
        {:error,
         "Filesystem cache policy requires valid start/end timestamps with end at or after start"}
    end
  end

  @spec form_values(map() | nil) :: map()
  def form_values(nil) do
    %{
      "enabled" => false,
      "timestamp_format" => "iso8601",
      "window_threshold" => 24,
      "window_unit" => "hours"
    }
  end

  def form_values(policy) when is_map(policy) do
    policy
    |> Map.put_new("enabled", true)
    |> Map.put_new("window_threshold", policy["bypass_fill_above_seconds"])
    |> Map.put_new("window_unit", "seconds")
  end

  def form_values(_policy), do: form_values(nil)

  @spec validate_mapping(Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  defp validate_mapping(changeset, policy) do
    with :ch_sql <- get_field(changeset, :language),
         {:ok, params} <-
           Sql.parameters(get_field(changeset, :query) || "", dialect: "clickhouse"),
         true <- policy["start_param"] in params and policy["end_param"] in params do
      put_change(changeset, :filesystem_cache_policy, policy)
    else
      _ ->
        add_error(
          changeset,
          :filesystem_cache_policy,
          "requires a ClickHouse endpoint with both time parameters declared in its SQL"
        )
    end
  end

  @spec valid_param?(term()) :: boolean()
  defp valid_param?(param) when is_binary(param), do: Regex.match?(~r/\A\w+\z/, param)
  defp valid_param?(_param), do: false

  @spec threshold(map()) :: {:ok, pos_integer()} | :error
  defp threshold(%{"window_threshold" => amount, "window_unit" => unit}) do
    with {:ok, amount} <- positive_integer(amount),
         multiplier when is_integer(multiplier) <- @units[unit],
         threshold when threshold <= @max_threshold <- amount * multiplier do
      {:ok, threshold}
    else
      _ -> :error
    end
  end

  defp threshold(policy), do: positive_integer(policy["bypass_fill_above_seconds"])

  @spec positive_integer(term()) :: {:ok, pos_integer()} | :error
  defp positive_integer(value) when is_integer(value) and value > 0 and value <= @max_threshold,
    do: {:ok, value}

  defp positive_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> positive_integer(integer)
      _ -> :error
    end
  end

  defp positive_integer(_value), do: :error

  @spec timestamp(term(), String.t()) :: {:ok, integer()} | :error
  defp timestamp(value, "iso8601") when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime, :microsecond)}
      _ -> :error
    end
  end

  defp timestamp(value, format) when format in ["unix_seconds", "unix_milliseconds"] do
    unit = if format == "unix_seconds", do: :second, else: :millisecond

    with {:ok, integer} <- unix_integer(value),
         {:ok, datetime} <- DateTime.from_unix(integer, unit) do
      {:ok, DateTime.to_unix(datetime, :microsecond)}
    else
      _ -> :error
    end
  end

  defp timestamp(_value, _format), do: :error

  @spec unix_integer(term()) :: {:ok, integer()} | :error
  defp unix_integer(value) when is_integer(value), do: {:ok, value}

  defp unix_integer(value) when is_binary(value) do
    case Integer.parse(value) do
      {integer, ""} -> {:ok, integer}
      _ -> :error
    end
  end

  defp unix_integer(_value), do: :error
end
