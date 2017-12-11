//
//  PostgresResult.swift
//  PostgreSQL
//
//  Created by Andrew J Wagner on 12/10/17.
//

import Foundation
import SQL
import CPostgreSQL

public final class PostgresResultDataProvider: ResultDataProvider {
    public var pointer: OpaquePointer?

    init(pointer: OpaquePointer?) {
        self.pointer = pointer
    }

    public var countAffected: Int {
        guard let raw = PQcmdTuples(self.pointer) else {
            return 0
        }
        return Int(String(cString: raw)) ?? 0
    }

    lazy var columns: [String:Int32] = {
        var output = [String:Int32]()
        for i in 0 ..< self.numberOfColumns {
            output[self.name(atColumn: i)!] = i
        }
        return output
    }()

    public func rows<Query>() -> RowSequence<Query> where Query: RowReturningQuery {
        return PostgresRowSequence(resultProvider: self)
    }
}

public final class PostgresRowSequence<Query: RowReturningQuery>: RowSequence<Query> {
    let resultProvider: PostgresResultDataProvider

    init(resultProvider: PostgresResultDataProvider) {
        self.resultProvider = resultProvider

        super.init()
    }

    public override var count: Int {
        return Int(PQntuples(self.resultProvider.pointer))
    }

    public override subscript(i: Int) -> Row<Query> {
        if i >= self.count {
            fatalError("Out of bounds")
        }
        return PostgresRow<Query>(resultProvider: self.resultProvider, row: Int32(i))
    }
}

// MARK: Internal

enum ResultStatus {
    case emptyQuery
    case commandOk
    case tuplesOk
    case copyOut
    case copyIn
    case badResponse
    case nonFatalError
    case fatalError
    case copyBoth
    case singleTuple
}

extension PostgresResultDataProvider {
    var internalStatus: ResultStatus {
        return ResultStatus(PQresultStatus(self.pointer))
    }

    var errorMessage: String? {
        guard let message = PQresultErrorMessage(self.pointer) else {
            return nil
        }
        return String(cString: message)
    }

    var errorDescription: String {
        let output = self.internalStatus.summary
        guard let message = self.errorMessage else {
            return output
        }
        return "\(output) - \(message)"
    }
}

extension PostgresResultDataProvider {
    var numberOfColumns: Int32 {
        return PQnfields(self.pointer)
    }

    func name(atColumn column: Int32) -> String? {
        guard let raw = PQfname(self.pointer, column) else {
            return nil
        }
        return String(cString: raw)
    }
}

extension ResultStatus {
    var summary: String {
        switch self {
        case .emptyQuery:
            return "Empty Query: The string sent to the server was empty."
        case .badResponse:
            return "Bad Response: The server's response was not understood."
        case .commandOk:
            return "Command OK: Successful completion of a command returning no data."
        case .tuplesOk:
            return "Tuples OK: Successful completion of a command returning data."
        case .copyOut:
            return "Copy Out: Data transfer started from server."
        case .copyIn:
            return "Copy In: Data transfer started to server."
        case .nonFatalError:
            return "Nonfatal Error: A nonfatal error (a notice or warning) occurred."
        case .fatalError:
            return "Fatal Error: A fatal error occured."
        case .copyBoth:
            return "Copy In/Out: Data transfer started to and from the server"
        case .singleTuple:
            return "Single Tuple: A single result was returned."
        }
    }

    init(_ type: ExecStatusType) {
        switch type {
        case PGRES_EMPTY_QUERY:
            self = .emptyQuery
        case PGRES_COMMAND_OK:
            self = .commandOk
        case PGRES_TUPLES_OK:
            self = .tuplesOk
        case PGRES_COPY_OUT:
            self = .copyOut
        case PGRES_COPY_IN:
            self = .copyIn
        case PGRES_BAD_RESPONSE:
            self = .badResponse
        case PGRES_NONFATAL_ERROR:
            self = .nonFatalError
        case PGRES_FATAL_ERROR:
            self = .fatalError
        case PGRES_COPY_BOTH:
            self = .copyBoth
        case PGRES_SINGLE_TUPLE:
            self = .singleTuple
        default:
            Swift.fatalError("Unknown Status Type")
        }
    }
}
