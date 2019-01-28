defmodule LogflareWeb.RuleController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  plug LogflareWeb.Plugs.RequireAuth when action in [:new, :create, :delete, :index]

  alias Logflare.Rule
  alias Logflare.Source
  alias Logflare.Repo

  def new(conn, %{"source_id" => source_id}) do
    changeset = Rule.changeset(%Rule{source: source_id })
    user_id = conn.assigns.user.id
    query = from s in "sources",
          where: s.user_id == ^user_id,
          select: %{
            name: s.name,
            id: s.id,
            token: s.token,
          }
    sources =
      for source <- Repo.all(query) do
        token = Ecto.UUID.load(source.token) |> elem(1)
        Map.put(source, :token, token)
      end
    source = Repo.get(Source, source_id)

    render(conn, "new.html", changeset: changeset, source: source, sources: sources)
  end

  def create(conn, %{"rule" => rule}) do
    source_id = rule["source"]
    source = Repo.get(Source, source_id)
    changeset = Repo.get(Source, rule["source"])
    |> Ecto.build_assoc(:rules)
    |> Rule.changeset(rule)
    user_id = conn.assigns.user.id
    query = from s in "sources",
        where: s.user_id == ^user_id,
        select: %{
          name: s.name,
          id: s.id,
          token: s.token,
        }
    sources =
      for source <- Repo.all(query) do
        token = Ecto.UUID.load(source.token) |> elem(1)
        Map.put(source, :token, token)
      end

    case Repo.insert(changeset) do
      {:ok, _rule} ->
        conn
        |> put_flash(:info, "Rule created successfully!")
        |> redirect(to: Routes.source_rule_path(conn, :index, source_id))
      {:error, %Ecto.Changeset{} = changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> render("new.html", changeset: changeset, source: source, sources: sources)
    end
  end

  def index(conn, %{"source_id" => source_id}) do
    source_id_int = String.to_integer(source_id)
    query = from r in "rules",
      where: r.source_id == ^source_id_int,
      select: %{
        regex: r.regex,
        sink: r.sink,
      }
    rules =
      for rule <- Repo.all(query) do
        sink = Ecto.UUID.load(rule.sink) |> elem(1)
        Map.put(rule, :sink, sink)
      end

    source = Repo.get(Source, source_id)

    render conn, "index.html", rules: rules, source: source
  end

end
