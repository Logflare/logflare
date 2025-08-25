defmodule Logflare.Google.BigQuery.GenUtils do
  @moduledoc """
  Generic utils for BigQuery.
  """

  require Logger

  import Ecto.Query
  import Logflare.Utils.Guards

  alias GoogleApi.BigQuery.V2.Connection
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.JSON
  alias Logflare.Repo
  alias Logflare.Source
  alias Logflare.Sources
  alias Logflare.User
  alias Logflare.Users

  @default_dataset_location "US"
  @table_ttl 604_800_000

  @doc """
  Returns the default TTL used (in days) for initializing the table.
  """
  def default_table_ttl_days do
    @table_ttl / :timer.hours(24)
  end

  @spec get_project_id(source_id :: atom()) :: String.t()
  def get_project_id(source_id) when is_atom_value(source_id) do
    %Source{user_id: user_id} = Sources.Cache.get_by(token: source_id)
    %User{bigquery_project_id: project_id} = Users.Cache.get(user_id)

    project_id || env_project_id()
  end

  @spec get_bq_user_info(source_id :: atom()) :: map()
  def get_bq_user_info(source_id) when is_atom_value(source_id) do
    %Source{user_id: user_id, bigquery_table_ttl: ttl} = Sources.Cache.get_by(token: source_id)

    %User{
      id: user_id,
      sources: _sources,
      bigquery_project_id: project_id,
      bigquery_dataset_location: dataset_location,
      bigquery_dataset_id: dataset_id
    } = Users.Cache.get(user_id)

    new_ttl =
      cond do
        is_nil(project_id) -> @table_ttl
        is_nil(ttl) -> @table_ttl
        true -> ttl * 86_400_000
      end

    new_project_id = project_id || env_project_id()
    new_dataset_location = dataset_location || @default_dataset_location
    new_dataset_id = dataset_id || "#{user_id}" <> env_default_table_name_append()

    %{
      user_id: user_id,
      bigquery_table_ttl: new_ttl,
      bigquery_project_id: new_project_id,
      bigquery_dataset_location: new_dataset_location,
      bigquery_dataset_id: new_dataset_id
    }
  end

  @spec format_table_name(source_id :: atom()) :: String.t()
  def format_table_name(source_id) when is_atom_value(source_id) do
    source_id
    |> Atom.to_string()
    |> String.replace("-", "_")
  end

  @doc """
  Dynamically builds a Tesla client connection. Switches adapter at runtime based on first arg.

  Uses `Logflare.FinchDefault` by default
  """
  @typep conn_type :: :ingest | {:query, User.t()} | :default
  @spec get_conn(conn_type()) :: Tesla.Env.client()
  def get_conn(conn_type \\ :default) do
    system_managed_sa_enabled = BigQueryAdaptor.managed_service_accounts_enabled?()
    # use pid as the partition hash
    {use_managed_sa?, partition_count} =
      case conn_type do
        {:query,
         %_{bigquery_project_id: project_id, bigquery_enable_managed_service_accounts: true}}
        when system_managed_sa_enabled == true and project_id != nil ->
          {true, BigQueryAdaptor.managed_service_account_partition_count()}

        {:query, %_{bigquery_project_id: nil}}
        when system_managed_sa_enabled == true ->
          {true, BigQueryAdaptor.managed_service_account_partition_count()}

        _ ->
          {false, BigQueryAdaptor.ingest_service_account_partition_count()}
      end

    partition = :erlang.phash2(self(), partition_count)

    {name, metadata} =
      if use_managed_sa? == true do
        pool_size = BigQueryAdaptor.managed_service_account_pool_size()

        sa_index = :erlang.phash2(self(), pool_size)

        {{
           Logflare.GothQuery,
           sa_index,
           partition
         },
         %{
           pool_size: pool_size,
           sa_index: sa_index,
           partition: partition
         }}
      else
        {{
           Logflare.Goth,
           partition
         },
         %{
           partition: partition
         }}
      end

    :telemetry.span([:logflare, :goth, :fetch], metadata, fn ->
      result = Goth.fetch(name)
      {result, metadata}
    end)
    |> case do
      {:ok, %Goth.Token{} = goth} ->
        Connection.new(goth.token)

      {:error, reason} ->
        Logger.error("Goth error!", error_string: inspect(reason))
        # This is going to give us an unauthorized connection but we are handling it downstream.
        Connection.new("")
    end
    # dynamically set tesla adapter
    |> Map.update!(:adapter, fn _value -> build_tesla_adapter_call(conn_type) end)
  end

  @spec get_account_id(source_id :: atom()) :: String.t()
  def get_account_id(source_id) when is_atom_value(source_id) do
    %Logflare.Source{user_id: account_id} = Sources.Cache.get_by(token: source_id)
    "#{account_id}"
  end

  @spec maybe_parse_google_api_result({:ok, any()} | {:error, any()}) ::
          {:ok, any()} | {:error, any()}
  def maybe_parse_google_api_result({:error, %Tesla.Env{} = teslaenv}) do
    {:error, teslaenv}
  end

  def maybe_parse_google_api_result(x), do: x

  @spec get_tesla_error_message(:emfile | :timeout | :closed | Tesla.Env.t()) :: String.t()
  def get_tesla_error_message(%Tesla.Env{} = message) do
    case JSON.decode(message.body) do
      {:ok, body} ->
        body["error"]["message"]

      {:error, data} ->
        inspect(data)
    end
  end

  def get_tesla_error_message(:emfile), do: "emfile"
  def get_tesla_error_message(:timeout), do: "timeout"
  def get_tesla_error_message(:closed), do: "closed"
  def get_tesla_error_message(message), do: inspect(message)

  @doc """
  Formats a label to be used as a key or value in Google Cloud resources.

  Keys and values can contain only lowercase letters, numeric characters, underscores, and dashes.
  All characters must use UTF-8 encoding, and international characters are allowed.
  Keys must start with a lowercase letter or international character.

  https://cloud.google.com/resource-manager/docs/labels-overview

  ## Examples

  iex> Logflare.Google.BigQuery.GenUtils.format_key("123label")
  "label"
  """
  @spec format_key(String.t() | integer() | atom()) :: String.t()
  def format_key(k) when is_binary(k) do
    k |> String.replace(~r/^[^[:alpha:]]+/u, "") |> format_value()
  end

  def format_key(k) when is_integer(k), do: k |> Integer.to_string() |> format_key()
  def format_key(k) when is_atom_value(k), do: k |> Atom.to_string() |> format_key()

  @doc """
  Formats values for BigQuery label values. Values are like keys except they can start
  with an integer.

  ## Examples

  iex> Logflare.Google.BigQuery.GenUtils.format_value("My Label 123")
  "my_label_123"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("Label-With-Dash")
  "label-with-dash"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("LABEL_with_MIXED_case")
  "label_with_mixed_case"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("123label")
  "123label"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("ключ")
  "ключ"

  iex> Logflare.Google.BigQuery.GenUtils.format_value(:atom_label)
  "atom_label"

  iex> Logflare.Google.BigQuery.GenUtils.format_value(42)
  "42"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("label with spaces and-dash")
  "label_with_spaces_and-dash"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("2025-08-19T14:59:02.111Z")
  "1755615542111"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("SomeVeryLongStringOfTextThatExceedsSixtyThreeCharactersInLengthXYZ")
  "someverylongstringoftextthatexceedssixtythreecharactersinlength"

  iex> Logflare.Google.BigQuery.GenUtils.format_value("My-Label With$Weird#Chars")
  "my-label_withweirdchars"
  """
  @spec format_value(String.t() | integer() | atom()) :: String.t()
  def format_value(v) when is_binary(v) do
    v =
      case DateTime.from_iso8601(v) do
        {:ok, datetime, _} -> DateTime.to_unix(datetime, :millisecond) |> Integer.to_string()
        _ -> v
      end

    v
    |> String.downcase()
    |> String.replace(" ", "_")
    |> String.replace(~r/[^[:alnum:]_-]/u, "")
    |> String.slice(0, 63)
  end

  def format_value(v) when is_integer(v), do: v |> Integer.to_string() |> format_value()
  def format_value(v) when is_atom_value(v), do: v |> Atom.to_string() |> format_value()

  @doc """
  Processes BigQuery error messages to make them more user-friendly.
  """
  @spec process_bq_errors(error :: map() | atom(), user_id :: integer()) :: map()
  def process_bq_errors(error, user_id) when is_atom_value(error) do
    %{"message" => process_bq_error_msg(error, user_id)}
  end

  def process_bq_errors(error, user_id) when is_map(error) do
    error = %{error | "message" => process_bq_error_msg(error["message"], user_id)}

    if is_list(error["errors"]) do
      %{
        error
        | "errors" => Enum.map(error["errors"], fn err -> process_bq_errors(err, user_id) end)
      }
    else
      error
    end
  end

  @spec process_bq_error_msg(message :: atom() | String.t(), user_id :: integer()) ::
          atom() | String.t()
  defp process_bq_error_msg(message, _user_id) when is_atom_value(message), do: message

  defp process_bq_error_msg(message, user_id) when is_binary(message) do
    regex =
      ~r/#{env_project_id()}\.#{user_id}_#{env()}\.(?<uuid>[0-9a-fA-F]{8}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{4}_[0-9a-fA-F]{12})/

    case Regex.named_captures(regex, message) do
      %{"uuid" => uuid} ->
        uuid = String.replace(uuid, "_", "-")

        query =
          from(s in Source,
            where: s.token == ^uuid and s.user_id == ^user_id,
            select: s.name
          )

        case Repo.one(query) do
          nil -> message
          source_name -> String.replace(message, regex, source_name)
        end

      nil ->
        message
    end
  end

  @spec env_project_id() :: String.t()
  defp env_project_id, do: Application.get_env(:logflare, Logflare.Google)[:project_id]

  @spec env_default_table_name_append() :: String.t()
  defp env_default_table_name_append do
    Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] || ""
  end

  # copy over runtime adapter building from Tesla.client/2
  # https://github.com/elixir-tesla/tesla/blob/v1.7.0/lib/tesla/builder.ex#L206
  @spec build_tesla_adapter_call(term()) :: Tesla.Adapter.t()
  defp build_tesla_adapter_call(:ingest) do
    Tesla.client(
      [],
      {Tesla.Adapter.Finch,
       name: Logflare.FinchIngest, pool_timeout: 2_500, receive_timeout: 5_000}
    ).adapter
  end

  defp build_tesla_adapter_call({:query, _}) do
    Tesla.client(
      [],
      {Tesla.Adapter.Finch, name: Logflare.FinchQuery, receive_timeout: 60_000}
    ).adapter
  end

  # use adapter in config.exs
  defp build_tesla_adapter_call(_), do: nil

  @spec env() :: String.t()
  defp env, do: Application.get_env(:logflare, :env)
end
