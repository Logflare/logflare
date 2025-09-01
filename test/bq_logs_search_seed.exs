alias Logflare.Sources
alias Logflare.User
alias Logflare.Users
alias Logflare.Logs.Search
alias Logflare.Logs.SearchOperations.SearchOperation, as: SO
alias Logflare.Sources.Source.BigQuery.SchemaBuilder
alias Logflare.Google.BigQuery
alias Logflare.Google.BigQuery.GenUtils
alias Logflare.Sources.Source.BigQuery.Pipeline
alias Logflare.Repo
alias Logflare.LogEvent
import Ecto.Query

email = System.get_env("LOGFLARE_TEST_USER_WITH_SET_IAM")
bigquery_dataset_location = "US"
bigquery_project_id = "logflare-dev-238720"
bigquery_table_ttl = 60 * 60 * 24 * 365
source_token = "2e051ba4-50ab-4d2a-b048-0dc595bfd6cf"

# {_, user} =
Users.insert_or_update_user(%{
  id: 314_159,
  email: email,
  bigquery_dataset_location: bigquery_dataset_location,
  provider: "google",
  provider_uid: "000000",
  token: "token",
  api_key: "api_key",
  name: "Test source name"
})

user =
  User
  |> where([u], u.email == ^email)
  |> Repo.one()

BigQuery.delete_dataset(user)

# Repo.delete!(user)

# {:ok, source} =
Sources.create_source(
  %{"token" => source_token, "name" => "Automated testing source #1"},
  user
)

source = Sources.get_by(token: source_token)

%{bigquery_dataset_id: bq_dataset_id} = GenUtils.get_bq_user_info(source.token)

BigQuery.init_table!(
  user.id,
  String.to_atom(source_token),
  bigquery_project_id,
  bigquery_table_ttl,
  bigquery_dataset_location,
  bq_dataset_id
)

schema =
  SchemaBuilder.build_table_schema(
    %{
      "int_field_1" => 1,
      "float_field_1" => 1.0
    },
    SchemaBuilder.initial_table_schema()
  )

{:ok, _} = BigQuery.patch_table(source.token, schema, bq_dataset_id, bigquery_project_id)

les =
  for x <- 1..5, y <- 100..101, h <- 1..10, m <- 1..10 do
    LogEvent.make(
      %{
        "_PARTITIONTIME" => Timex.shift(Timex.today(), days: -1),
        "message" => "x#{x} y#{y}",
        "metadata" => %{
          "int_field_1" => 1,
          "float_field_1" => 1.0
        },
        "timestamp" => ~U[2020-01-01T12:00:00Z] |> Timex.shift(hours: h, minutes: m) |> to_string
      },
      %{source: source}
    )
  end

bq_rows = Enum.map(les, &Pipeline.le_to_bq_row/1)
project_id = GenUtils.get_project_id(source.token)

{:ok, _} =
  BigQuery.stream_batch!(
    %{
      bigquery_project_id: project_id,
      bigquery_dataset_id: bq_dataset_id,
      source_id: source.token
    },
    bq_rows
  )
