//
//  PostgreRow.swift
//  PostgreSQL
//
//  Created by Andrew J Wagner on 12/10/17.
//

import Foundation
import SQL
import CPostgreSQL

public final class PostgresRow<Query: RowReturningQuery>: Row<Query> {
    let resultProvider: PostgresResultDataProvider
    let row: Int32

    init(resultProvider: PostgresResultDataProvider, row: Int32) {
        self.row = row
        self.resultProvider = resultProvider

        super.init()
    }

    public override var columns: [String] {
        return Array(self.resultProvider.columns.keys)
    }

    public override func data(forColumnNamed name: String) throws -> Data? {
        guard let column = self.resultProvider.columns[name]
            , PQgetisnull(self.resultProvider.pointer, self.row, column) == 0
            else
        {
            return nil
        }

        let length = PQgetlength(self.resultProvider.pointer, self.row, column)
        guard length > 0, let raw = PQgetvalue(self.resultProvider.pointer, self.row, column) else {
            return Data()
        }

        let data = Data(bytes: raw, count: Int(length))
        switch PQftype(self.resultProvider.pointer, column) {
        case 17:
            guard let hex = String(data: data, encoding: .utf8)
                , let data = Data(hexString: hex)
                else
            {
                throw SQLError(message: "invalid data for '\(name)'")
            }
            return data
        default:
            return data
        }
    }
}

private extension Data {
    init?(hexString: String) {
        guard hexString.hasPrefix("\\x") else {
            return nil
        }
        let length = (hexString.count - 2) / 2
        var data = Data(capacity: length)
        for i in 0 ..< length {
            let j = hexString.index(hexString.startIndex, offsetBy: i * 2 + 2)
            let k = hexString.index(j, offsetBy: 2)
            let bytes = hexString[j..<k]
            if var byte = UInt8(bytes, radix: 16) {
                data.append(&byte, count: 1)
            } else {
                return nil
            }
        }
        self = data
    }
}
