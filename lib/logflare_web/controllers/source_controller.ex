defmodule LogflareWeb.SourceController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug(LogflareWeb.Plugs.CheckSourceCount when action in [:new, :create])

  plug(
    LogflareWeb.Plugs.VerifySourceOwner
    when action in [:show, :edit, :update, :delete, :clear_logs, :favorite]
  )

  alias Logflare.Source
  alias Logflare.Repo
  alias LogflareWeb.AuthController
  alias Logflare.SourceData
  alias Logflare.TableManager
  alias Number.Delimit
  alias Logflare.Google.BigQuery
  alias Logflare.User

  @project_id Application.get_env(:logflare, Logflare.Google)[:project_id]
  @dataset_id_append Application.get_env(:logflare, Logflare.Google)[:dataset_id_append]

  def dashboard(conn, _params) do
    user_id = conn.assigns.user.id
    user_email = conn.assigns.user.email

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: [desc: s.favorite],
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          favorite: s.favorite
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.load(source.token)

        rate = Delimit.number_to_delimited(SourceData.get_rate(source))
        timestamp = SourceData.get_latest_date(source)
        average_rate = Delimit.number_to_delimited(SourceData.get_avg_rate(source))
        max_rate = Delimit.number_to_delimited(SourceData.get_max_rate(source))
        buffer_count = Delimit.number_to_delimited(SourceData.get_buffer(token))
        event_inserts = Delimit.number_to_delimited(SourceData.get_total_inserts(token))

        source
        |> Map.put(:rate, rate)
        |> Map.put(:token, token)
        |> Map.put(:latest, timestamp)
        |> Map.put(:avg, average_rate)
        |> Map.put(:max, max_rate)
        |> Map.put(:buffer, buffer_count)
        |> Map.put(:inserts, event_inserts)
      end

    render(conn, "dashboard.html", sources: sources, user_email: user_email)
  end

  def favorite(conn, %{"id" => source_id}) do
    old_source = Repo.get(Source, source_id)
    source = %{"favorite" => !old_source.favorite}
    changeset = Source.changeset(old_source, source)

    case Repo.update(changeset) do
      {:ok, _source} ->
        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      {:error, _changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
  end

  def new(conn, _params) do
    changeset = Source.changeset(%Source{}, %{})
    render(conn, "new.html", changeset: changeset)
  end

  def create(conn, %{"source" => source}) do
    user = conn.assigns.user

    changeset =
      user
      |> Ecto.build_assoc(:sources)
      |> Source.changeset(source)

    oauth_params = get_session(conn, :oauth_params)

    case Repo.insert(changeset) do
      {:ok, _source} ->
        TableManager.new_table(String.to_atom(source["token"]))

        case is_nil(oauth_params) do
          true ->
            conn
            |> put_flash(:info, "Source created!")
            |> redirect(to: Routes.source_path(conn, :dashboard))

          false ->
            conn
            |> put_flash(:info, "Source created!")
            |> AuthController.redirect_for_oauth(user)
        end

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("new.html", changeset: changeset)
    end
  end

  def show(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    table_id = String.to_atom(source.token)
    user_id = conn.assigns.user.id
    user_email = conn.assigns.user.email

    bigquery_project_id =
      if conn.assigns.user.bigquery_project_id do
        conn.assigns.user.bigquery_project_id
      else
        @project_id
      end

    explore_link = generate_explore_link(user_id, user_email, table_id, bigquery_project_id)

    logs =
      Enum.map(SourceData.get_logs(table_id), fn log ->
        if Map.has_key?(log, :metadata) do
          {:ok, encoded} = Jason.encode(log.metadata, pretty: true)
          %{log | metadata: encoded}
        else
          log
        end
      end)

    render(conn, "show.html",
      logs: logs,
      source: source,
      public_token: nil,
      explore_link: explore_link
    )
  end

  def public(conn, %{"public_token" => public_token}) do
    source = Repo.get_by(Source, public_token: public_token)

    explore_link = ""

    case source == nil do
      true ->
        conn
        |> put_flash(:error, "Public path not found!")
        |> redirect(to: Routes.marketing_path(conn, :index))

      false ->
        table_id = String.to_atom(source.token)

        logs =
          Enum.map(SourceData.get_logs(table_id), fn log ->
            if Map.has_key?(log, :metadata) do
              {:ok, encoded} = Jason.encode(log.metadata, pretty: true)
              %{log | metadata: encoded}
            else
              log
            end
          end)

        render(conn, "show.html",
          logs: logs,
          source: source,
          public_token: public_token,
          explore_link: explore_link
        )
    end
  end

  def edit(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    user_id = conn.assigns.user.id
    changeset = Source.changeset(source, %{})
    disabled_source = source.token
    avg_rate = SourceData.get_avg_rate(String.to_atom(source.token))

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          overflow_source: s.overflow_source
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.load(source.token)
        s = Map.put(source, :token, token)

        if disabled_source == token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    render(conn, "edit.html",
      changeset: changeset,
      source: source,
      sources: sources,
      avg_rate: avg_rate
    )
  end

  def update(conn, %{"id" => source_id, "source" => updated_params}) do
    old_source = Repo.get(Source, source_id)
    changeset = Source.changeset(old_source, updated_params)
    user_id = conn.assigns.user.id
    disabled_source = old_source.token
    avg_rate = SourceData.get_avg_rate(String.to_atom(old_source.token))

    query =
      from(s in "sources",
        where: s.user_id == ^user_id,
        order_by: s.name,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
          overflow_source: s.overflow_source
        }
      )

    sources =
      for source <- Repo.all(query) do
        {:ok, token} = Ecto.UUID.load(source.token)
        s = Map.put(source, :token, token)

        if disabled_source == token,
          do: Map.put(s, :disabled, true),
          else: Map.put(s, :disabled, false)
      end

    case Repo.update(changeset) do
      {:ok, source} ->
        case updated_params do
          %{"bigquery_table_ttl" => ttl} ->
            %Logflare.User{bigquery_project_id: project_id} = Repo.get(User, user_id)

            ttl = String.to_integer(ttl) * 86_400_000
            BigQuery.patch_table_ttl(String.to_atom(source.token), ttl, project_id)

          _ ->
            nil
        end

        conn
        |> put_flash(:info, "Source updated!")
        |> redirect(to: Routes.source_path(conn, :edit, source_id))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("edit.html",
          changeset: changeset,
          source: old_source,
          sources: sources,
          avg_rate: avg_rate
        )
    end
  end

  def delete(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)

    case :ets.info(String.to_atom(source.token)) do
      :undefined ->
        source |> Repo.delete!()

        conn
        |> put_flash(:info, "Source deleted!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      _ ->
        case :ets.first(String.to_atom(source.token)) do
          :"$end_of_table" ->
            {:ok, _table} = TableManager.delete_table(String.to_atom(source.token))
            source |> Repo.delete!()

            conn
            |> put_flash(:info, "Source deleted!")
            |> redirect(to: Routes.source_path(conn, :dashboard))

          {timestamp, _unique_int, _monotime} ->
            now = System.os_time(:microsecond)

            if now - timestamp > 3_600_000_000 do
              {:ok, _table} = TableManager.delete_table(String.to_atom(source.token))
              source |> Repo.delete!()

              conn
              |> put_flash(:info, "Source deleted!")
              |> redirect(to: Routes.source_path(conn, :dashboard))
            else
              conn
              |> put_flash(
                :error,
                "Failed! Recent events found. Latest event must be greater than 24 hours old."
              )
              |> redirect(to: Routes.source_path(conn, :dashboard))
            end
        end
    end
  end

  def clear_logs(conn, %{"id" => source_id}) do
    source = Repo.get(Source, source_id)
    {:ok, _table} = TableManager.reset_table(String.to_atom(source.token))

    conn
    |> put_flash(:info, "Logs cleared!")
    |> redirect(to: Routes.source_path(conn, :show, source_id))
  end

  defp generate_explore_link(
         user_id,
         user_email,
         table_id,
         project_id
         # billing_project_id
       ) do
    dataset_id = Integer.to_string(user_id) <> @dataset_id_append

    {:ok, explore_link_config} =
      Jason.encode(%{
        "projectId" => project_id,
        "tableId" => BigQuery.format_table_name(table_id),
        "datasetId" => dataset_id,
        # billingProjectId" => billing_project_id,
        "connectorType" => "BIG_QUERY",
        "sqlType" => "STANDARD_SQL"
      })

    explore_link_prefix = "https://datastudio.google.com/explorer?authuser=#{user_email}&config="

    explore_link_prefix <> URI.encode(explore_link_config)
  end
end
