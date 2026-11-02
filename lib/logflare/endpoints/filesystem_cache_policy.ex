defmodule Logflare.Endpoints.FilesystemCachePolicy do
  @moduledoc """
  Endpoint-owned time-window rules for passive ClickHouse filesystem-cache reads.
  Rules are versioned with the SQL whose parameters they describe.
  """

  import Ecto.Changeset
  import Logflare.Utils.Guards, only: [is_pos_integer: 1]

  alias Logflare.Sql

  @setting "read_from_filesystem_cache_if_exists_otherwise_bypass_cache"
  @formats ~w(iso8601 unix_seconds unix_milliseconds)
  @units %{"seconds" => 1, "minutes" => 60, "hours" => 3_600, "days" => 86_400}
  @keys ~w(start_param end_param timestamp_format bypass_fill_above_seconds enabled window_threshold window_unit)
  @max_threshold 9_223_372_036_854

  @type decision :: %{window_seconds: number() | nil, settings: map()}
  @type error ::
          {:invalid_filesystem_cache_policy | :invalid_filesystem_cache_window, String.t()}

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
        changeset = put_change(changeset, :filesystem_cache_policy, policy)

        cond do
          get_field(changeset, :language) != :ch_sql and
              changed?(changeset, :filesystem_cache_policy) ->
            add_error(
              changeset,
              :filesystem_cache_policy,
              "is only supported for ClickHouse endpoints"
            )

          get_field(changeset, :language) != :ch_sql ->
            put_change(changeset, :filesystem_cache_policy, nil)

          Enum.any?([:query, :language, :filesystem_cache_policy], &changed?(changeset, &1)) ->
            validate_mapping(changeset, policy)

          true ->
            changeset
        end

      {:error, message} ->
        add_error(changeset, :filesystem_cache_policy, message)
    end
  end

  @spec resolve(map() | nil, map()) :: {:ok, decision()} | {:error, error()}
  def resolve(policy, params) do
    case normalize(policy) do
      {:ok, nil} ->
        {:ok, %{window_seconds: nil, settings: %{}}}

      {:ok, policy} ->
        resolve_window(policy, params)

      {:error, message} ->
        {:error, {:invalid_filesystem_cache_policy, "Filesystem cache policy " <> message}}
    end
  end

  @spec resolve_window(map(), map()) :: {:ok, decision()} | {:error, error()}
  defp resolve_window(policy, params) do
    with {:ok, start} <- timestamp(params[policy["start_param"]], policy["timestamp_format"]),
         {:ok, finish} <- timestamp(params[policy["end_param"]], policy["timestamp_format"]),
         true <- is_nil(start) or is_nil(finish) or finish >= start do
      {:ok, window_decision(policy, start, finish)}
    else
      _ ->
        {:error,
         {:invalid_filesystem_cache_window,
          "Filesystem cache policy requires valid start/end timestamps with end at or after start"}}
    end
  end

  @spec window_decision(map(), integer() | nil, integer() | nil) :: decision()
  defp window_decision(_policy, start, finish) when is_nil(start) or is_nil(finish),
    do: %{window_seconds: nil, settings: %{}}

  defp window_decision(policy, start, finish) do
    window = finish - start

    settings =
      if window > policy["bypass_fill_above_seconds"] * 1_000_000,
        do: %{@setting => 1},
        else: %{}

    %{window_seconds: window / 1_000_000, settings: settings}
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
    form_values(nil)
    |> Map.put("enabled", true)
    |> Map.merge(threshold_form_values(policy))
    |> Map.merge(policy)
  end

  def form_values(_policy), do: form_values(nil)

  @spec threshold_form_values(map()) :: map()
  defp threshold_form_values(%{"bypass_fill_above_seconds" => seconds}),
    do: %{"window_threshold" => seconds, "window_unit" => "seconds"}

  defp threshold_form_values(_policy), do: %{}

  @spec validate_mapping(Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  defp validate_mapping(changeset, policy) do
    if Keyword.has_key?(changeset.errors, :query) do
      changeset
    else
      with {:ok, params} <-
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
  defp positive_integer(value) when is_pos_integer(value) and value <= @max_threshold,
    do: {:ok, value}

  defp positive_integer(value) when is_binary(value) do
    with {:ok, integer} <- unix_integer(value), do: positive_integer(integer)
  end

  defp positive_integer(_value), do: :error

  @spec timestamp(term(), String.t()) :: {:ok, integer() | nil} | :error
  defp timestamp(nil, _format), do: {:ok, nil}
  defp timestamp("", _format), do: {:ok, nil}

  defp timestamp(value, "iso8601") when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} -> {:ok, DateTime.to_unix(datetime, :microsecond)}
      _ -> :error
    end
  end

  defp timestamp(value, "unix_seconds"), do: unix_timestamp(value, :second)
  defp timestamp(value, "unix_milliseconds"), do: unix_timestamp(value, :millisecond)
  defp timestamp(_value, _format), do: :error

  @spec unix_timestamp(term(), :second | :millisecond) :: {:ok, integer()} | :error
  defp unix_timestamp(value, unit) do
    with {:ok, integer} <- unix_integer(value),
         {:ok, datetime} <- DateTime.from_unix(integer, unit) do
      {:ok, DateTime.to_unix(datetime, :microsecond)}
    else
      _ -> :error
    end
  end

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
