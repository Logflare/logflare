# This configuration file is used for Scout APM.
# See our help docs at http://help.apm.scoutapp.com/#elixir-agent for more information.
# config/scout_apm.exs
use Mix.Config

config :phoenix, :template_engines,
  eex: ScoutApm.Instruments.EExEngine,
  exs: ScoutApm.Instruments.ExsEngine,
  log_level: "debug"
