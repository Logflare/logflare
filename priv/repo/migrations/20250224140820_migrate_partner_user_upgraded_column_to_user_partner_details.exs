defmodule Logflare.Repo.Migrations.MigratePartnerUserUpgradedColumnToUserPartnerDetails do
  use Ecto.Migration

  def up do
    execute("""
      UPDATE users AS u
      SET
        partner_id = pu.partner_id,
        partner_details =
        COALESCE(u.partner_details, '{}'::jsonb) || jsonb_set('{}'::jsonb, '{upgraded}', to_jsonb(pu.upgraded))
      FROM partner_users AS pu
      WHERE pu.user_id = u.id;
    """)
  end

  def down do
    execute("""
    INSERT INTO partner_users (user_id, partner_id, upgraded)
    SELECT u.id,
           u.partner_id,
           CASE
             WHEN u.partner_details is null THEN false
             WHEN u.partner_details->>'upgraded' = 'true' THEN true
             WHEN u.partner_details->>'upgraded' = 'false' THEN false
             ELSE NULL
           END
    FROM users u
    WHERE u.partner_id is not null
    ON CONFLICT (user_id, partner_id)
    DO UPDATE SET upgraded = EXCLUDED.upgraded; -- Update the existing record
  """)

  end
end
