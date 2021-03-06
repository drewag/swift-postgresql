//
//  PostgreSQLConnection.swift
//  PostgreSQL
//
//  Created by Andrew J Wagner on 12/10/17.
//

import Foundation
import SQL
import CPostgreSQL

public final class PostgreSQLConnection: Connection {
    var pointer: OpaquePointer?

    let host: String
    let port: Int
    let databaseName: String
    let username: String
    let password: String

    public var isConnected: Bool {
        return self.pointer != nil
    }

    public init(
        host: String,
        port: Int = 5432,
        databaseName: String,
        username: String,
        password: String
        )
    {
        self.host = host
        self.port = port
        self.databaseName = databaseName
        self.username = username
        self.password = password
    }

    deinit {
        self.disconnect()
    }

    public func error(_ message: String?) -> SQLError {
        return SQLError(connection: self.pointer, message: message)
    }

    public func connect() throws {
        guard !self.isConnected else {
            return
        }

        let port = "\(self.port)"
        let newConnection = PQsetdbLogin(
            self.host,
            port,
            nil,
            nil,
            self.databaseName,
            self.username,
            self.password
        )

        guard PQstatus(newConnection) == CONNECTION_OK else {
            PQfinish(newConnection)
            throw SQLError(connection: newConnection)
        }

        self.pointer = newConnection
    }

    public func disconnect() {
        guard let pointer = self.pointer else {
            return
        }

        PQfinish(pointer)
        self.pointer = nil
    }

    public func run(_ statement: String, arguments: [Value]) throws {
        let pointer = try self.execute(statement: statement, arguments: arguments)
        let result = PostgresResultDataProvider(pointer: pointer)
        switch result.internalStatus {
        case .emptyQuery, .badResponse, .nonFatalError, .fatalError:
            throw self.error(result.errorDescription)
        case .commandOk, .tuplesOk, .copyOut, .copyIn, .copyBoth, .singleTuple:
            break
        }
    }

    public func execute<Query: AnyQuery>(_ query: Query) throws -> Result<Query> {
        let pointer = try self.execute(statement: query.statement, arguments: query.arguments)
        let provider = PostgresResultDataProvider(pointer: pointer)
        switch provider.internalStatus {
        case .commandOk:
            return Result(dataProvider: provider, query: query)
        case .nonFatalError, .fatalError, .badResponse:
            throw self.error(provider.errorDescription)
        default:
            throw self.error("Unexpected status: '\(provider.internalStatus.summary)'")
        }
    }

    public func execute<Query: RowReturningQuery>(_ query: Query) throws -> RowsResult<Query> {
        let pointer = try self.execute(statement: query.statement, arguments: query.arguments)
        let provider = PostgresResultDataProvider(pointer: pointer)
        switch provider.internalStatus {
        case .tuplesOk:
            return RowsResult(dataProvider: provider, query: query)
        case .nonFatalError, .fatalError, .badResponse:
            throw self.error(provider.errorDescription)
        default:
            throw self.error("Unexpected status: '\(provider.internalStatus.summary)'")
        }
    }

    internal func replaceParametersWithNumbers(in statement: String) -> String {
        let dateFormat: String
        #if os(Linux)
            dateFormat = "'YYYY-MM-DD HH24:MI:SS.USZ'"
        #else
            dateFormat = "'YYYY-MM-DDTHH24:MI:SS.USZ'"
        #endif
        let statement = statement
            .renamingFunction(
                named: "====to_timestamp====",
                to: "to_timestamp",
                addingParameters: [dateFormat]
            )
            .renamingFunction(
                named: "====to_local_timestamp====",
                to: "to_timestamp",
                addingParameters: [dateFormat]
            )
            .replacingOccurrences(of: "====data_type====", with: "bytea")
        var output: String = ""
        var varCount = 1
        for component in statement.components(separatedBy: "%@") {
            if !output.isEmpty {
                output += "$\(varCount)"
                varCount += 1
            }

            output += component
        }

        return output
    }
}

private extension PostgreSQLConnection {
    func execute(statement: String, arguments: [Value]) throws -> OpaquePointer? {
        try self.connect()

        var parameterData = [UnsafePointer<Int8>?]()
        var parameterFormats = [Int32]()
        var lengths = [Int32]()
        var deallocators = [() -> ()]()
        var temps = [[UInt8]]()
        defer { deallocators.forEach { $0() } }

        for parameter in arguments {
            let data: AnyCollection<Int8>
            switch parameter {
            case .null:
                parameterData.append(nil)
                parameterFormats.append(0)
                lengths.append(0)
                continue
            case .bool(let value):
                data = AnyCollection((value ? "true" : "false").utf8CString)
            case .float(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .double(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .int(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .int8(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .int16(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .int32(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .int64(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .uint(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .uint8(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .uint16(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .uint32(let value):
                data = AnyCollection("\(value)".utf8CString)
            case .uint64(let value):
                data = AnyCollection("\(value)".utf8CString)
            case let .point(x, y):
                data = AnyCollection("\(x),\(y)".utf8CString)
            case let .time(hour, minute, second):
                data = AnyCollection("\(hour):\(minute):\(second)".utf8CString)
            case .string(let string):
                data = AnyCollection(string.utf8CString)
            case .data(let raw):
                let bytes = raw.map { $0 }
                temps.append(bytes)
                parameterData.append(UnsafePointer<Int8>(OpaquePointer(temps.last!)))
                parameterFormats.append(1)
                lengths.append(Int32(raw.count))
                continue
            }

            let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: data.count)
            deallocators.append {
                pointer.deallocate()
            }

            for (index, byte) in data.enumerated() {
                pointer[index] = byte
            }

            parameterData.append(pointer)
            parameterFormats.append(0)
            lengths.append(0)
        }

        let statement = self.replaceParametersWithNumbers(in: statement)
//        let paramStrings: [String] = parameterData.map({ pointer in
//            guard let pointer = pointer else {
//                return "NULL"
//            }
//            return String(cString: pointer)
//        })
//        print(statement)
//        print(paramStrings)

        return PQexecParams(
            self.pointer,
            statement,
            Int32(parameterData.count),
            nil,
            parameterData,
            &lengths,
            &parameterFormats,
            0
        )
    }
}

private extension String {
    func renamingFunction(named old: String, to new: String, addingParameters: [String]) -> String {
        var updated = self
        while let range = updated.range(of: old + "(") {
            var afterThisRange = updated
            afterThisRange.replaceSubrange(range, with: "\(new)(")

            var index = afterThisRange.index(range.lowerBound, offsetBy: new.count + 1)
            var openCount = 1
            findClose: while index != afterThisRange.endIndex {
                switch afterThisRange[index] {
                case "(":
                    openCount += 1
                case ")":
                    openCount -= 1
                    if openCount == 0 {
                        break findClose
                    }
                default:
                    break
                }

                index = afterThisRange.index(after: index)
            }

            guard index != afterThisRange.endIndex else {
                return updated
            }

            updated = afterThisRange
            guard !addingParameters.isEmpty else {
                continue
            }
            updated.insert(contentsOf: "," + addingParameters.joined(separator: ","), at: index)
        }
        return updated
    }
}
