from conan import ConanFile
from conan.tools.cmake import cmake_layout


class PerspectiveServerConan(ConanFile):
    name = "perspective-server"
    version = "4.3.0"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps", "VirtualBuildEnv"

    def requirements(self):
        # Pin versions that have pre-built binaries on conancenter
        # for MSVC 194, shared=False, runtime=dynamic, cppstd=14/17.
        # Only Arrow builds from source (due to with_csv=True).
        self.requires("arrow/18.1.0")
        self.requires("protobuf/5.27.0")
        self.requires("re2/20240702")
        self.requires("rapidjson/cci.20230929")
        self.requires("boost/1.86.0")
        self.requires("date/3.0.3")
        self.requires("tsl-hopscotch-map/2.3.1")
        self.requires("tsl-ordered-map/1.1.0")
        self.requires("exprtk/0.0.2")

        # Force abseil to match pre-built re2/protobuf binaries.
        self.requires("abseil/20250127.0", force=True)

    def configure(self):
        # Arrow: enable CSV (forces source build for Arrow only).
        # All other options match the conancenter pre-built binary
        # so transitive deps (boost, thrift, zlib, etc.) are pre-built.
        self.options["arrow"].with_csv = True
        self.options["arrow"].with_json = False
        self.options["arrow"].with_flight_rpc = False
        self.options["arrow"].gandiva = False
        self.options["arrow"].with_re2 = False
        self.options["arrow"].with_utf8proc = False
        self.options["arrow"].with_brotli = False
        self.options["arrow"].with_bz2 = False
        self.options["arrow"].with_lz4 = False
        self.options["arrow"].with_snappy = False
        self.options["arrow"].with_zstd = False
        # Keep parquet, with_thrift, with_zlib, with_boost at defaults
        # to match pre-built binary and avoid dep conflicts.

    def layout(self):
        cmake_layout(self)
