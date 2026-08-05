// swift-tools-version: 6.3
import PackageDescription

let package = Package(
    name: "swift-connection-pool",
    platforms: [
        .macOS(.v26), .iOS(.v26)
    ],
    products: [
        .library(
            name: "ConnectionPool",
            targets: ["ConnectionPool"]
        ),
    ],
    targets: [
        .target(
            name: "ConnectionPool"
        ),
        .testTarget(
            name: "ConnectionPoolTests",
            dependencies: ["ConnectionPool"]
        ),
    ],
    swiftLanguageModes: [.v6]
)
