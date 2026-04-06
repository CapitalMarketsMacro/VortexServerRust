from conan import ConanFile
from conan.tools.cmake import cmake_layout


class PerspectiveServerConan(ConanFile):
    name = "perspective-server"
    version = "4.3.0"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps", "VirtualBuildEnv"

    def requirements(self):
        # Latest versions with pre-built MSVC 194 binaries on conancenter.
        # ALL deps download as pre-built — zero source compilation.
        self.requires("arrow/22.0.0")
        self.requires("protobuf/6.33.5")
        self.requires("re2/20251105")
        self.requires("abseil/20260107.1", force=True)
        self.requires("rapidjson/cci.20230929")
        self.requires("boost/1.86.0")
        self.requires("date/3.0.4")
        self.requires("tsl-hopscotch-map/2.3.1")
        self.requires("tsl-ordered-map/1.1.0")
        self.requires("exprtk/0.0.2")

    def configure(self):
        # Arrow: disable optional features that pull in deps with
        # restricted download URLs (thrift requires archive.apache.org).
        # This forces Arrow to build from source but avoids network issues.
        self.options["arrow"].with_thrift = False
        self.options["arrow"].parquet = False

    def layout(self):
        cmake_layout(self)
