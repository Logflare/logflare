defmodule LogflareWeb.UserController do
  use LogflareWeb, :controller
  use Phoenix.HTML

  alias Logflare.{User, Repo}
  alias Logflare.Google.BigQuery
  alias Logflare.Google.CloudResourceManager
  alias Logflare.Source.Supervisor

  @service_account Application.get_env(:logflare, Logflare.Google)[:service_account] || ""

  def edit(%{assigns: %{user: user}} = conn, _params) do
    changeset = User.update_by_user_changeset(user, %{})

    render(conn, "edit.html",
      changeset: changeset,
      user: user,
      service_account: @service_account
    )
  end

  def update(conn, %{"user" => params}) do
    user = conn.assigns.user
    prev_bigquery_project_id = user.bigquery_project_id

    user
    |> User.update_by_user_changeset(params)
    |> Repo.update()
    |> case do
      {:ok, user} ->
        new_bq_project? = user.bigquery_project_id != prev_bigquery_project_id

        if new_bq_project?, do: Supervisor.reset_all_user_sources(user)

        conn
        |> put_flash(:info, "Account updated!")
        |> put_flash(:new_bq_project, new_bq_project?)
        |> redirect(to: Routes.user_path(conn, :edit))

      {:error, changeset} ->
        conn
        |> put_flash(:error, "Something went wrong!")
        |> put_status(406)
        |> render("edit.html",
          changeset: changeset,
          user: conn.assigns.user,
          service_account: @service_account
        )
    end
  end

  def delete(%{assigns: %{user: user}} = conn, _params) do
    # TODO: soft delete, delayed deleted

    Supervisor.delete_all_user_sources(user)
    BigQuery.delete_dataset(user)
    Repo.delete!(user)

    spawn(fn ->
      CloudResourceManager.set_iam_policy()
    end)

    conn
    |> put_flash(:info, "Account deleted!")
    |> redirect(to: Routes.marketing_path(conn, :index))
  end

  def new_api_key(conn, _params) do
    case conn.params["undo"] do
      "true" ->
        %{assigns: %{user: user}} = conn
        new_api_key = user.old_api_key
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, auth_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, "API key restored!")
        |> redirect(to: Routes.source_path(conn, :dashboard))

      nil ->
        %{assigns: %{user: user}} = conn
        new_api_key = :crypto.strong_rand_bytes(12) |> Base.url_encode64() |> binary_part(0, 12)
        old_api_key = user.api_key
        auth_params = %{api_key: new_api_key, old_api_key: old_api_key}

        changeset = User.changeset(user, auth_params)
        Repo.update(changeset)

        conn
        |> put_flash(:info, [
          "API key reset! ",
          link("Undo?", to: Routes.user_path(conn, :new_api_key, undo: true))
        ])
        |> redirect(to: Routes.source_path(conn, :dashboard))
    end
  end
end
