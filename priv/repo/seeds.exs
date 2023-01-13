# Script for populating the database. You can run it as:
#
#     mix run priv/repo/seeds.exs
#
# Inside the script, you can read and write to any of your
# repositories directly:
#
#     Logflare.Repo.insert!(%Logflare.SomeSchema{})
#
# We recommend using the bang functions (`insert!`, `update!`
# and so on) as they will fail if something goes wrong.

Ecto.Adapters.SQL.query!(Logflare.Repo, """
INSERT INTO "public"."plans"("id","name","stripe_id","inserted_at","updated_at","period","price","limit_sources","limit_rate_limit","limit_alert_freq","limit_source_rate_limit","limit_saved_search_limit","limit_team_users_limit","limit_source_fields_limit","limit_source_ttl","type")
VALUES
(7,E'Free',NULL,E'2020-05-12 21:36:00',E'2020-08-26 14:19:11',E'month',0,100,10,14400000,5,1,2,500,259200000,E'standard'),
(9,E'Hobby',E'price_1Gy1BNLvvReWx3FxdfprtwOO',E'2020-06-25 20:21:09',E'2020-08-26 14:23:21',E'month',500,100,250,3600000,50,1,2,500,604800000,E'standard'),
(10,E'Hobby',E'price_1Gy1BNLvvReWx3Fx6CxvaPTK',E'2020-06-25 20:22:24',E'2020-08-26 14:23:26',E'year',5000,100,250,3600000,50,1,2,500,604800000,E'standard'),
(11,E'Pro',E'price_1Gy1CpLvvReWx3FxCRaW2p9J',E'2020-06-25 20:24:12',E'2020-08-26 14:22:38',E'month',800,100,100000,900000,50000,1,2,500,2592000000,E'standard'),
(12,E'Pro',E'price_1Gy1CpLvvReWx3FxvspnYC0Q',E'2020-06-25 20:25:23',E'2020-08-26 14:23:34',E'year',8000,100,100000,900000,50000,1,2,500,2592000000,E'standard'),
(13,E'Business',E'price_1Gy1E4LvvReWx3FxREuLO3gA',E'2020-06-25 20:26:53',E'2020-08-26 14:24:16',E'month',1200,100,1000,60000,50,1,2,500,5184000000,E'standard'),
(14,E'Business',E'price_1Gy1E5LvvReWx3FxsAiv3DjB',E'2020-06-25 20:27:51',E'2020-08-26 14:24:20',E'year',12000,100,1000,60000,50,1,2,500,5184000000,E'standard'),
(15,E'Enterprise',E'price_1GyJkCLvvReWx3FxPzVKzUlU',E'2020-06-26 15:50:34',E'2020-08-26 14:24:26',E'year',20000,100,5000,1000,100,1,2,500,5184000000,E'standard'),
(16,E'Enterprise',E'price_1GyJkCLvvReWx3Fx6WXoATHQ',E'2020-06-26 15:51:16',E'2020-08-26 14:24:33',E'month',2000,100,5000,1000,100,1,2,500,5184000000,E'standard'),
(17,E'Lifetime',E'price_1HJhDzLvvReWx3Fx5J5mMzEj',E'2020-08-24 15:07:31',E'2020-08-26 14:24:56',E'life',50000,8,250,60000,25,10,9,500,5184000000,E'standard'),
(20,E'Enterprise Metered BYOB',E'price_1IB4mjLvvReWx3FxtUoyGNnZ',E'2021-01-20 22:56:43',E'2021-01-20 22:57:39',E'month',10000,100,1000,60000,1000,10,10,500,5184000000,E'metered'),
(21,E'Enterprise Metered',E'price_1IB4ZrLvvReWx3Fxzhf9vo25',E'2021-01-22 15:11:38',E'2021-01-22 15:11:38',E'month',10000,100,1000,60000,1000,10,10,500,5184000000,E'metered'),
(22,E'Metered',E'price_1Jn5crLvvReWx3Fx09NdM5ki',E'2021-07-12 14:23:07',E'2021-10-21 18:05:18',E'month',1500,100,1000,60000,1000,10,10,500,5184000000,E'metered'),
(23,E'Metered BYOB',E'price_1Jn59kLvvReWx3FxPBXNS4Me',E'2021-07-12 20:29:40',E'2021-10-21 17:36:08',E'month',1000,100,1000,60000,1000,10,10,500,5184000000,E'metered');
""")
