defmodule LogflareWeb.AdminController do
  use LogflareWeb, :controller

  import Ecto.Query, only: [from: 2]

  alias Logflare.Admin
  alias Logflare.Repo
  alias Logflare.User
  alias Logflare.Users
  alias LogflareWeb.AuthController

  require Logger

  @page_size 50
  @accounts_sort_options [
    :inserted_at,
    :updated_at
  ]
  defp env_node_shutdown_code, do: Application.get_env(:logflare, :node_shutdown_code)

  def dashboard(conn, _params) do
    conn
    |> render("dashboard.html")
  end

  def accounts(conn, params) do
    accounts = paginate_accounts(params)

    conn
    |> assign(:accounts, accounts)
    |> assign(:sort_options, @accounts_sort_options)
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

    # clear the user session and cookies
    conn =
      conn
      |> delete_session(:team_user_id)
      |> delete_session(:last_switched_team_id)
      |> delete_resp_cookie("_logflare_user_id")
      |> delete_resp_cookie("_logflare_team_user_id")

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

  def shutdown_node(conn, params) do
    if Map.get(params, "code") == env_node_shutdown_code() do
      do_authorized_code_shutdown(conn, params)
    else
      do_unauthorized_code_shutdown(conn, params)
    end
  end

  defp do_authorized_code_shutdown(conn, %{"node" => node}) do
    node_names = [Node.self() | Node.list()]
    nodes = node_names |> Enum.map(&Atom.to_string/1)

    if Enum.member?(nodes, node) do
      node_name =
        Enum.find(node_names, fn nn ->
          Atom.to_string(nn) == node
        end)

      Admin.shutdown(node_name)

      conn
      |> put_status(:ok)
      |> json(%{"message" => "Success, shutting down node: #{node}"})
    else
      Logger.warning("Node shutdown requested!")

      conn
      |> put_status(:unauthorized)
      |> json(%{
        "message" => "Error, valid node required!",
        "nodes" => nodes,
        "current_node" => Node.self()
      })
    end
  end

  defp do_authorized_code_shutdown(conn, _params) do
    Admin.shutdown()

    conn
    |> put_status(:ok)
    |> json(%{"message" => "Success, shutting down node: #{Node.self()}"})
  end

  defp do_unauthorized_code_shutdown(conn, _params) do
    Logger.warning("Node shutdown requested!")

    conn
    |> put_status(:unauthorized)
    |> json(%{"message" => "Error, valid shutdown code required!"})
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

  defp query_accounts do
    from u in User,
      order_by: [desc: :inserted_at],
      select: u,
      preload: :billing_account,
      limit: 100
  end

  defp query_accounts(sort_by) when is_binary(sort_by) do
    from u in User,
      order_by: [desc: ^sort_option_to_atom(sort_by)],
      select: u,
      preload: :billing_account,
      limit: 100
  end

  defp query_accounts(email, sort_by) when is_binary(sort_by) do
    e = "%#{email}%"

    from u in User,
      order_by: [desc: ^sort_option_to_atom(sort_by)],
      where: ilike(u.email, ^e),
      select: u,
      preload: :billing_account,
      limit: 100
  end

  defp sort_option_to_atom(option) when is_binary(option) do
    Enum.find(@accounts_sort_options, &(Atom.to_string(&1) == option))
  end
end
