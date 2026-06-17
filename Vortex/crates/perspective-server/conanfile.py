from conan import ConanFile
from conan.tools.cmake import cmake_layout


class PerspectiveServerConan(ConanFile):
    name = "perspective-server"
    version = "4.3.0"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeToolchain", "CMakeDeps", "VirtualBuildEnv"

    def requirements(self):
        # Every dependency below resolves to a PRE-BUILT binary on
        # ConanCenter for our target profiles (Linux gcc 13, Windows
        # msvc 194, macOS apple-clang 17) — nothing compiles from source.
        # The exact, drift-proof binary set is pinned in conan.lock next
        # to this file; build.rs / build.sh / build.bat pass --lockfile so
        # a new ConanCenter recipe revision can never silently flip a
        # dependency back to a source build. See CLAUDE.md ("C++
        # dependencies: pre-built only").
        #
        # arrow uses ConanCenter's DEFAULT options (parquet=True,
        # with_thrift=True) on purpose: that is the only option set for
        # which ConanCenter publishes a pre-built arrow binary. thrift
        # itself is also pre-built, so enabling it costs nothing and never
        # reaches archive.apache.org. boost is pinned to 1.90.0 to match
        # the boost version arrow's pre-built binary was linked against
        # (a different boost major.minor changes arrow's package_id and
        # loses the pre-built match). Perspective uses boost header-only
        # (multi_index, dynamic_bitset, functional/hash, uuid), so the
        # version is not otherwise constrained.
        self.requires("arrow/22.0.0")
        self.requires("protobuf/6.33.5")
        self.requires("re2/20251105")
        self.requires("abseil/20260107.1", force=True)
        self.requires("rapidjson/cci.20230929")
        self.requires("boost/1.90.0")
        self.requires("date/3.0.4")
        self.requires("tsl-hopscotch-map/2.3.1")
        self.requires("tsl-ordered-map/1.1.0")
        self.requires("exprtk/0.0.2")

    def layout(self):
        cmake_layout(self)
