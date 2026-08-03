defmodule Logflare.Repo.Migrations.MigratePartnerUserUpgradedColumnToUserPartnerDetails do
  use Ecto.Migration

  def up do
    execute("""
      UPDATE users AS u
      SET
        partner_id = pu.partner_id,
        partner_upgraded = pu.upgraded
      FROM partner_users AS pu
      WHERE pu.user_id = u.id;
    """)
  end

  def down do
    execute("""
    INSERT INTO partner_users (user_id, partner_id, upgraded)
    SELECT u.id,
           u.partner_id,
           u.partner_upgraded
    FROM users u
    WHERE u.partner_id is not null
    ON CONFLICT (user_id, partner_id)
    DO UPDATE SET upgraded = EXCLUDED.upgraded;
    """)

  end
end
