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

        if Query.self is RowReturningQuery.Type {
            switch provider.internalStatus {
            case .tuplesOk:
                return Result(dataProvider: provider, query: query)
            case .nonFatalError, .fatalError, .badResponse:
                throw self.error(provider.errorDescription)
            default:
                throw self.error("Unexpected status: '\(provider.internalStatus.summary)'")
            }
        }
        else {
            switch provider.internalStatus {
            case .commandOk:
                return Result(dataProvider: provider, query: query)
            case .nonFatalError, .fatalError, .badResponse:
                throw self.error(provider.errorDescription)
            default:
                throw self.error("Unexpected status: '\(provider.internalStatus.summary)'")
            }
        }
    }
}

private extension PostgreSQLConnection {
    func execute(statement: String, arguments: [Value]) throws -> OpaquePointer? {
        try self.connect()

        var parameterData = [UnsafePointer<Int8>?]()
        var deallocators = [() -> ()]()
        defer { deallocators.forEach { $0() } }

        for parameter in arguments {
            let data: AnyCollection<Int8>
            switch parameter {
            case .null:
                parameterData.append(nil)
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
            case .string(let string):
                data = AnyCollection(string.utf8CString)
            case .data(let data):
                data.withUnsafeBytes({ pointer in
                    parameterData.append(pointer)
                })
                continue
            }

            let pointer = UnsafeMutablePointer<Int8>.allocate(capacity: Int(data.count))
            deallocators.append {
                pointer.deallocate()
            }

            for (index, byte) in data.enumerated() {
                pointer[index] = byte
            }

            parameterData.append(pointer)
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
            nil,
            nil,
            0
        )
    }

    func replaceParametersWithNumbers(in statement: String) -> String {
        let statement = statement.replacingOccurrences(
            of: "====to_timestamp====(%@)",
            with: "to_timestamp(%@, 'YYYY-MM-DD HH24:MI:SS.USZ')"
        )
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
