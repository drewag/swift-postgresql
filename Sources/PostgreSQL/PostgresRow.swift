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

    public override subscript(string: String) -> Data? {
        guard let column = self.resultProvider.columns[string]
            , PQgetisnull(self.resultProvider.pointer, self.row, column) == 0
            else
        {
            return nil
        }

        let length = PQgetlength(self.resultProvider.pointer, self.row, column)
        guard length > 0, let raw = PQgetvalue(self.resultProvider.pointer, self.row, column) else {
            return Data()
        }

        return Data(bytes: raw, count: Int(length))
    }
}
