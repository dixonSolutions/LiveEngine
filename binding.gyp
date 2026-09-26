{
  "targets": [
    {
      "target_name": "liveengine",
      "sources": [ "src/core/engine.cpp", "src/addon/binding.cc" ],
      "include_dirs": [ "<!@(node -p \"require('node-addon-api').include\")", "src/core" ],
      "defines": [ "NAPI_VERSION=8", "NODE_ADDON_API_DISABLE_DEPRECATED" ],
      "cflags!": [ "-fno-exceptions" ],
      "cflags_cc!": [ "-fno-exceptions" ],
      "cflags_cc": [ "-std=c++17", "-O2" ],
      "xcode_settings": {
        "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
        "CLANG_CXX_LIBRARY": "libc++",
        "CLANG_CXX_LANGUAGE_STANDARD": "c++17",
        "MACOSX_DEPLOYMENT_TARGET": "10.15"
      },
      "msvs_settings": {
        "VCCLCompilerTool": { "ExceptionHandling": 1, "AdditionalOptions": [ "/std:c++17" ] }
      }
    }
  ]
}
