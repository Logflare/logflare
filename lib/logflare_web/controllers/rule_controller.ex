defmodule LogflareWeb.RuleController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug LogflareWeb.Plugs.RequireAuth when action in [:create, :delete, :index]

  alias Logflare.Rule
  alias Logflare.Source
  alias Logflare.Repo

  def create(conn, %{"rule" => rule}) do
    source_id = rule["source"]
    source = Repo.get(Source, source_id)
    changeset = Repo.get(Source, rule["source"])
    |> Ecto.build_assoc(:rules)
    |> Rule.changeset(rule)
    user_id = conn.assigns.user.id
    source_query = from s in "sources",
        where: s.user_id == ^user_id,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
        }
    sources =
      for source <- Repo.all(source_query) do
        token = Ecto.UUID.load(source.token) |> elem(1)
        Map.put(source, :token, token)
      end

    source_id_int = String.to_integer(source_id)
    rules_query = from r in "rules",
      where: r.source_id == ^source_id_int,
      select: %{
        id: r.id,
        regex: r.regex,
        sink: r.sink,
      }

    rules =
      for rule <- Repo.all(rules_query) do
        sink = Ecto.UUID.load(rule.sink) |> elem(1)
        Map.put(rule, :sink, sink)
      end

    case Repo.insert(changeset) do
      {:ok, _rule} ->
        conn
        |> put_flash(:info, "Rule created successfully!")
        |> redirect(to: Routes.source_rule_path(conn, :index, source_id))
      {:error, %Ecto.Changeset{} = changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("index.html", rules: rules, source: source, changeset: changeset, sources: sources)
        # |> redirect(to: Routes.source_rule_path(conn, :index, source_id))
    end
  end

  def index(conn, %{"source_id" => source_id}) do
    IO.inspect(conn)
    changeset = Rule.changeset(%Rule{source: source_id })
    user_id = conn.assigns.user.id
    source_id_int = String.to_integer(source_id)
    rules_query = from r in "rules",
      where: r.source_id == ^source_id_int,
      select: %{
        id: r.id,
        regex: r.regex,
        sink: r.sink,
      }
    source_query = from s in "sources",
        where: s.user_id == ^user_id,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
        }
    rules =
      for rule <- Repo.all(rules_query) do
        sink = Ecto.UUID.load(rule.sink) |> elem(1)
        Map.put(rule, :sink, sink)
      end

    source = Repo.get(Source, source_id)

    sources =
      for source <- Repo.all(source_query) do
        token = Ecto.UUID.load(source.token) |> elem(1)
        Map.put(source, :token, token)
      end

    render conn, "index.html", rules: rules, source: source, changeset: changeset, sources: sources
  end

  def delete(conn, %{"id" => rule_id, "source_id" => source_id}) do
    Repo.get!(Rule, rule_id) |> Repo.delete!

    conn
    |> put_flash(:info, "Rule deleted!")
    |> redirect(to: Routes.source_rule_path(conn, :index, source_id))
  end

end
