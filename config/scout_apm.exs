# This configuration file is used for Scout APM.
# See our help docs at http://help.apm.scoutapp.com/#elixir-agent for more information.
# config/scout_apm.exs
use Mix.Config

config :scout_apm,
  # The app name that will appear within the Scout UI
  name: "Logflare",
  key: "NHjQWbuKfOqkU5Wk5dTj"

config :phoenix, :template_engines,
  eex: ScoutApm.Instruments.EExEngine,
  exs: ScoutApm.Instruments.ExsEngine
