{
  description = "Development Environment";

  inputs = {
    beam-flakes = {
      url = "github:elixir-tools/nix-beam-flakes";
      inputs.flake-parts.follows = "flake-parts";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
  };

  outputs = inputs @ {
    beam-flakes,
    flake-parts,
    ...
  }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [beam-flakes.flakeModule];

      systems = [
        "aarch64-darwin"
        "x86_64-darwin"
        "x86_64-linux"
      ];

      perSystem = {pkgs, ...}: {
        formatter = pkgs.alejandra;

        beamWorkspace = {
          enable = true;
          devShell = {
            phoenix = true;
            extraArgs = {
              PLAYWRIGHT_NODEJS_PATH = "${pkgs.nodejs}/bin/node";
              PLAYWRIGHT_SKIP_VALIDATE_HOST_REQUIREMENTS = true;

              PLAYWRIGHT_CHROMIUM_PATH = "${pkgs.playwright-driver.browsers}/chromium-1181/chrome-linux/chrome";
              PLAYWRIGHT_FIREFOX_PATH = "${pkgs.playwright-driver.browsers}/firefox-1489/firefox/firefox";
              PLAYWRIGHT_WEBKIT_PATH = "${pkgs.playwright-driver.browsers}/webkit-2191/pw_run.sh";

            };
            extraPackages = with pkgs; [
              google-cloud-sdk
              cargo

              nodejs

              playwright
              playwright-driver.browsers
            ];
          };
          versions.fromToolVersions = ./.tool-versions;
        };
      };
    };
}
