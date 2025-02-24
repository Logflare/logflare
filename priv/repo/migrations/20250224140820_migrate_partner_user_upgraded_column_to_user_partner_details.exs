defmodule Logflare.Repo.Migrations.MigratePartnerUserUpgradedColumnToUserPartnerDetails do
  use Ecto.Migration

  def up do
    execute("""
      UPDATE users AS u
      SET partner_details = jsonb_set(u.partner_details, '{upgraded}', to_jsonb(pu.upgraded))
      FROM partner_users AS pu
      WHERE pu.user_id = u.id AND pu.upgraded IS NOT NULL;
    """)
  end

  def down do
    execute("""
      UPDATE partner_users pu
      SET upgraded = u.partner_details->>'upgraded'::boolean
      FROM users u
      WHERE pu.user_id = u.id AND u.partner_details->>'upgraded' IS NOT NULL;
    """)
  end
end
