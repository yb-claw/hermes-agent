# nix/python.nix — uv2nix virtual environment builder
{
  python311,
  lib,
  callPackage,
  uv2nix,
  pyproject-nix,
  pyproject-build-systems,
  stdenv,
}:
let
  workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ./..; };
  hacks = callPackage pyproject-nix.build.hacks { };

  overlay = workspace.mkPyprojectOverlay {
    sourcePreference = "wheel";
  };

  isAarch64Darwin = stdenv.hostPlatform.system == "aarch64-darwin";

  # Keep the workspace locked through uv2nix, but supply the local voice stack
  # from nixpkgs so wheel-only transitive artifacts do not break evaluation.
  mkPrebuiltPassthru = dependencies: {
    inherit dependencies;
    optional-dependencies = { };
    dependency-groups = { };
  };

  mkPrebuiltOverride = final: from: dependencies:
    hacks.nixpkgsPrebuilt {
      inherit from;
      prev = {
        nativeBuildInputs = [ final.pyprojectHook ];
        passthru = mkPrebuiltPassthru dependencies;
      };
    };

  # alibabacloud sdist-only packages that use setuptools but don't declare it
  # as a build dependency — provide it explicitly on all platforms.
  addSetuptools = final: prev:
    let
      needsSetuptools = [
        "alibabacloud-credentials-api"
        "alibabacloud-endpoint-util"
        "alibabacloud-gateway-dingtalk"
        "alibabacloud-gateway-spi"
        "alibabacloud-tea"
      ];
      mkOverride = name: {
        inherit name;
        value = prev.${name}.overrideAttrs (old: {
          nativeBuildInputs = (old.nativeBuildInputs or [ ]) ++ [ final.setuptools ];
        });
      };
    in
    builtins.listToAttrs (builtins.filter (o: prev ? ${o.name}) (map mkOverride needsSetuptools));

  pythonPackageOverrides = final: _prev:
    if isAarch64Darwin then {
      numpy = mkPrebuiltOverride final python311.pkgs.numpy { };

      av = mkPrebuiltOverride final python311.pkgs.av { };

      humanfriendly = mkPrebuiltOverride final python311.pkgs.humanfriendly { };

      coloredlogs = mkPrebuiltOverride final python311.pkgs.coloredlogs {
        humanfriendly = [ ];
      };

      onnxruntime = mkPrebuiltOverride final python311.pkgs.onnxruntime {
        coloredlogs = [ ];
        numpy = [ ];
        packaging = [ ];
      };

      ctranslate2 = mkPrebuiltOverride final python311.pkgs.ctranslate2 {
        numpy = [ ];
        pyyaml = [ ];
      };

      faster-whisper = mkPrebuiltOverride final python311.pkgs.faster-whisper {
        av = [ ];
        ctranslate2 = [ ];
        huggingface-hub = [ ];
        onnxruntime = [ ];
        tokenizers = [ ];
        tqdm = [ ];
      };
    } else {};

  pythonSet =
    (callPackage pyproject-nix.build.packages {
      python = python311;
    }).overrideScope
      (lib.composeManyExtensions [
        pyproject-build-systems.overlays.default
        overlay
        addSetuptools
        pythonPackageOverrides
      ]);
in
pythonSet.mkVirtualEnv "hermes-agent-env" {
  hermes-agent = [ "all" ];
}
