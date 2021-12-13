defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller
  import Ecto.Query, only: [from: 2]

  alias Logflare.{Repo, Source, Sources, User, Users}
  alias LogflareWeb.AuthController

  require Logger

  @page_size 50
  @node_shutdown_code Application.get_env(:logflare, :node_shutdown_code)

  def dashboard(conn, _params) do
    conn
    |> render("dashboard.html")
  end

  def accounts(conn, params) do
    sort_options = [
      :inserted_at,
      :updated_at
    ]

    accounts = paginate_accounts(params)

    conn
    |> assign(:accounts, accounts)
    |> assign(:sort_options, sort_options)
    |> render("accounts.html")
  end

  def become_account(conn, %{"id" => id}) do
    user = Users.get(id)

    auth_params = %{
      token: user.token,
      email: user.email,
      email_preferred: user.email_preferred,
      provider: user.provider,
      image: user.image,
      name: user.name,
      provider_uid: user.provider_uid
    }

    AuthController.check_invite_token_and_signin(conn, auth_params)
  end

  def delete_account(%{assigns: %{user: %User{email: "chase@logflare.app"}}} = conn, %{
        "id" => user_id
      }) do
    user = Users.get(user_id)

    case Users.delete_user(user) do
      {:ok, _user} ->
        conn
        |> put_flash(:info, "Account deleted!")
        |> redirect(to: Routes.admin_path(conn, :accounts))

      {:error, _reason} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> redirect(to: Routes.admin_path(conn, :accounts))
    end
  end

  def delete_account(conn, %{"id" => _user_id}) do
    conn
    |> put_flash(:error, "You are not chase@logflare.app!")
    |> redirect(to: Routes.admin_path(conn, :accounts))
  end

  def shutdown_node(conn, %{"code" => @node_shutdown_code, "node" => node} = params) do
    node = String.to_atom(node)
    nodes = Node.list()

    if Enum.member?([Node.self() | nodes], node) do
      Logger.warn("Node shutdown initiated!")
      Logflare.Admin.shutdown(node)

      conn
      |> put_status(:ok)
      |> json(%{"message" => "Success, shutting down node: #{node}"})
    else
      Logger.warn("Node shutdown requested!")

      conn
      |> put_status(:unauthorized)
      |> json(%{
        "message" => "Error, valid node required!",
        "nodes" => nodes,
        "current_node" => Node.self()
      })
    end
  end

  def shutdown_node(conn, %{"code" => @node_shutdown_code}) do
    Logger.warn("Node shutdown initiated!")

    Logflare.Admin.shutdown()

    conn
    |> put_status(:ok)
    |> json(%{"message" => "Success, shutting down node: #{Node.self()}"})
  end

  def shutdown_node(conn, params) do
    IO.inspect(params)
    Logger.warn("Node shutdown requested!")

    conn
    |> put_status(:unauthorized)
    |> json(%{"message" => "Error, valid shutdown code required!"})
  end

  def sources(conn, params) do
    sort_options = [
      :fields,
      :latest,
      :rejected,
      :rate,
      :avg,
      :max,
      :buffer,
      :inserts,
      :recent
    ]

    sorted_sources = sorted_sources(params)

    render(conn, "sources.html", sources: sorted_sources, sort_options: sort_options)
  end

  defp paginate_accounts(%{"page" => page, "sort_by" => ""}) do
    query_accounts()
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp paginate_accounts(%{"page" => page, "sort_by" => sort_by}) do
    query_accounts(sort_by)
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp paginate_accounts(%{"page" => page}) do
    query_accounts()
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp paginate_accounts(%{"sort_by" => sort_by}) do
    query_accounts(sort_by)
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp paginate_accounts(%{"email" => email}) do
    query_accounts(email, "inserted_at")
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp paginate_accounts(_params) do
    query_accounts()
    |> Repo.all()
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp sorted_sources(%{"page" => page, "sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: page})
  end

  defp sorted_sources(%{"sort_by" => sort_by} = _params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.sort_by(&Map.fetch(&1.metrics, String.to_atom(sort_by)), &>=/2)
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp sorted_sources(_params) do
    query()
    |> Repo.all()
    |> Stream.map(&Sources.refresh_source_metrics/1)
    |> Stream.map(&Sources.put_schema_field_count/1)
    |> Enum.into([])
    |> Repo.paginate(%{page_size: @page_size, page: 1})
  end

  defp query() do
    from s in Source,
      order_by: [desc: s.inserted_at],
      select: s
  end

  defp query_accounts() do
    from u in User,
      order_by: [desc: :inserted_at],
      select: u,
      preload: :billing_account
  end

  defp query_accounts(sort_by) when is_binary(sort_by) do
    from u in User,
      order_by: [desc: ^String.to_atom(sort_by)],
      select: u,
      preload: :billing_account
  end

  defp query_accounts(email, sort_by) when is_binary(sort_by) do
    e = "%#{email}%"

    from u in User,
      order_by: [desc: ^String.to_atom(sort_by)],
      where: ilike(u.email, ^e),
      select: u,
      preload: :billing_account
  end
end
