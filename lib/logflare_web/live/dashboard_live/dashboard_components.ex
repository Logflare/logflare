defmodule LogflareWeb.DashboardLive.DashboardComponents do
  use LogflareWeb, :html
  use LogflareWeb, :routes
  use Phoenix.Component

  alias Logflare.Auth

  def integrations(assigns) do
    ~H"""
    <div class="">
      <.integrations_list>
        <:heading>Integrations</:heading>
        <:item title="Cloudflare" link="https://cloudflareapps.com/apps/logflare" />
        <:item title="Vercel" link="https://docs.logflare.app/integrations/vercel/" />
        <:item title="Fly" link="https://github.com/Logflare/fly-log-shipper" />
        <:item title="Postgres FDW" link="https://docs.logflare.app/integrations/postgres-fdw" description="SQL" />
        <:item title="Pino Logflare" link="https://docs.logflare.app/integrations/pino-logflare" description="Javascript" />
        <:item title="LoggerBackend" link="https://github.com/Logflare/logflare_logger_backend" description="Elixir" />
        <:item title="logflare_erl" link="https://github.com/Logflare/logflare_erl" description="Erlang" />
      </.integrations_list>

      <.link class="tw-text-white tw-text-sm" href="https://github.com/Logflare/logflare#integrations">View all integrations</.link>

      <.integrations_list>
        <:heading>Documentation</:heading>
        <:item title="docs.logflare.app" link="https://docs.logflare.app" />
        <:item title="OpenAPI" link={~p"/swaggerui"} />
      </.integrations_list>
    </div>
    """
  end

  slot :heading, required: true

  slot :item do
    attr :title, :string, required: true
    attr :link, :string, required: true
    attr :description, :string, required: false
  end

  def integrations_list(assigns) do
    ~H"""
    <h5 class="header-margin"><%= render_slot(@heading) %></h5>
    <ul class="tw-list-none tw-p-0 tw-m-0 tw-mb-3">
      <li :for={item <- @item} class="tw-px-5 tw-py-2 tw-border-b tw-border-gray-200 tw-bg-[#1d1d1d] tw-text-sm">
        <.link class="tw-text-white" href={item.link}><%= item.title %></.link>
        <span :if={item[:description]} class="tw-text-xs tw-block">
          <%= item.description %>
        </span>
      </li>
    </ul>
    """
  end

  def teams(assigns) do
     ~H"""
     <div>
       <h5 class="header-margin">Teams</h5>
       <ul class="list-unstyled">
         <li :if={@home_team}>
           <strong :if={@current_team.id == @home_team.id}><%= @home_team.name %></strong>
           <.link :if={@current_team.id != @home_team.id} navigate={~p"/profile/switch?#{%{"user_id" => @home_team.user_id, "redirect_to" => "/dashboard_new"}}"}>
             <%= @home_team.name %>
           </.link>
           <small>home team</small>
         </li>

         <li :if={@home_team == nil}>
           <.link href={~p"/account"} method="post">
             Create your own Logflare account.
           </.link>
         </li>

         <%= if Enum.empty?(@team_users) do %>
           Other teams you are a member of will be listed here.
         <% end %>

         <li :for={team_user <- @team_users} :if={team_user.team.id != @current_team.id}>
           <li>
           <span :if={team_user.team_id == @current_team.id}><%= team_user.team.name %></span>
           <.link :if={team_user.team_id != @current_team.id} navigate={~p"/profile/switch?#{%{"user_id" => team_user.team.user_id, "team_user_id" => team_user.id, "redirect_to" => "/dashboard_new"}}"}>
               <%= team_user.team.name %>
             </.link>
           </li>
         </li>
       </ul>
     </div>
     """
   end

   def members(assigns) do
     ~H"""
     <div>
       <h5 class="header-margin">Members</h5>
       <ul id="team-members" class="tw-mb-1 tw-list-none tw-p-0">
         <li>
           <img class="rounded-circle" width="35" height="35" src={@user.image || Auth.gen_gravatar_link(@user.email)} alt={@user.name || @user.email} />
           <.link href={"mailto:#{@user.email}"}>
             <%= @user.name || @user.email %>
           </.link>
           <small><%= if true, do: "owner", else: "owner, you" %></small>
         </li>
         <li :for={member <- @team.team_users} :if={member.id != @user.id }>
           <img class="rounded-circle" width="35" height="35" src={member.image || Auth.gen_gravatar_link(member.email)} alt={member.name || member.email} />
           <.link href={"mailto:#{member.email}"}>
             <%= member.name || member.email %>
           </.link>

           <.link href={~p"/profile/#{member.id}"} data-confirm="Delete member?" class="dashboard-links" method="delete" >
             <i class="fa fa-trash"></i>
           </.link>
           </li>
       </ul>
       <.link href={~p"/account/edit#team-members"}>
         Invite more team members.
       </.link>
     </div>
     """
   end
end
