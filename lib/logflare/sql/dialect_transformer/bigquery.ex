defmodule Logflare.Sql.DialectTransformer.BigQuery do
  @moduledoc """
  BigQuery-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  import Logflare.Utils.Guards

  alias Logflare.User

  @impl true
  def quote_style, do: "`"

  @impl true
  def dialect, do: "bigquery"

  @impl true
  def transform_source_name(source_name, %{sources: sources} = data) do
    source = Enum.find(sources, fn s -> s.name == source_name end)

    token =
      source.token
      |> Atom.to_string()
      |> String.replace("-", "_")

    # byob bq
    project_id =
      if is_nil(data.user_project_id), do: data.logflare_project_id, else: data.user_project_id

    # byob bq
    dataset_id =
      if is_nil(data.user_dataset_id), do: data.logflare_dataset_id, else: data.user_dataset_id

    "#{project_id}.#{dataset_id}.#{token}"
  end

  @doc """
  Validates BigQuery-specific transformation data.
  """
  @spec validate_transformation_data(map()) :: :ok | {:error, String.t()}
  def validate_transformation_data(%{
        logflare_project_id: logflare_project_id,
        user_project_id: user_project_id,
        logflare_dataset_id: logflare_dataset_id,
        user_dataset_id: user_dataset_id
      })
      when is_non_empty_binary(logflare_project_id) and is_non_empty_binary(logflare_dataset_id) do
    cond do
      is_nil(user_project_id) and is_nil(user_dataset_id) ->
        :ok

      is_non_empty_binary(user_project_id) and is_non_empty_binary(user_dataset_id) ->
        :ok

      true ->
        {:error, "Invalid BigQuery project/dataset configuration"}
    end
  end

  def validate_transformation_data(_), do: {:error, "Missing BigQuery transformation data"}

  @doc """
  Builds transformation data for BigQuery from a user.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(
        %User{
          bigquery_project_id: user_project_id,
          bigquery_dataset_id: user_dataset_id
        } = user,
        base_data
      ) do
    Map.merge(base_data, %{
      logflare_project_id: Application.get_env(:logflare, Logflare.Google)[:project_id],
      user_project_id: user_project_id,
      logflare_dataset_id: User.generate_bq_dataset_id(user),
      user_dataset_id: user_dataset_id
    })
  end
end
