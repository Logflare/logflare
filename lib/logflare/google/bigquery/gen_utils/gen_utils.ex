defmodule Logflare.Google.BigQuery.GenUtils do
  @moduledoc """
  Generic utils for BigQuery.
  """
  require Logger

  alias Logflare.JSON
  alias Logflare.{Sources, Users}
  alias Logflare.{Source, User}
  alias GoogleApi.BigQuery.V2.Connection

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @table_ttl 604_800_000
  @default_dataset_location "US"
  @default_table_name_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append] ||
                               ""

  @spec get_project_id(atom()) :: String.t()
  def get_project_id(source_id) when is_atom(source_id) do
    %Source{user_id: user_id} = Sources.get_by(token: source_id)
    %User{bigquery_project_id: project_id} = Users.get_by(id: user_id)

    project_id || @project_id
  end

  @spec get_bq_user_info(atom) :: map
  def get_bq_user_info(source_id) when is_atom(source_id) do
    %Source{user_id: user_id, bigquery_table_ttl: ttl} = Sources.get_by(token: source_id)

    %User{
      id: user_id,
      sources: _sources,
      bigquery_project_id: project_id,
      bigquery_dataset_location: dataset_location,
      bigquery_dataset_id: dataset_id
    } = Users.get_by(id: user_id)

    new_ttl =
      cond do
        is_nil(project_id) -> @table_ttl
        is_nil(ttl) -> @table_ttl
        true -> ttl * 86_400_000
      end

    new_project_id = project_id || @project_id
    new_dataset_location = dataset_location || @default_dataset_location
    new_dataset_id = dataset_id || "#{user_id}" <> @default_table_name_append

    %{
      user_id: user_id,
      bigquery_table_ttl: new_ttl,
      bigquery_project_id: new_project_id,
      bigquery_dataset_location: new_dataset_location,
      bigquery_dataset_id: new_dataset_id
    }
  end

  @spec format_table_name(atom) :: String.t()
  def format_table_name(source) do
    string = Atom.to_string(source)
    String.replace(string, "-", "_")
  end

  def get_conn() do
    case Goth.fetch(Logflare.Goth) do
      {:ok, %Goth.Token{} = goth} ->
        Connection.new(goth.token)

      {:error, reason} ->
        Logger.error("Goth error!", error_string: inspect(reason))
        # This is going to give us an unauthorized connection but we are handling it downstream.
        Connection.new("")
    end
  end

  @spec get_account_id(atom) :: String.t()
  def get_account_id(source_id) when is_atom(source_id) do
    %Logflare.Source{user_id: account_id} = Sources.get_by(token: source_id)
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
end
