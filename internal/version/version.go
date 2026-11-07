// Package version holds the build version, set at link time:
//
//	go build -ldflags "-X github.com/chrisyifanjin/wsnap/internal/version.Version=v0.1.0"
package version

// Version is "dev" in builds without the link flag.
var Version = "dev"
