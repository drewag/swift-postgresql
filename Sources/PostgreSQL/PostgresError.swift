//
//  PostgresError.swift
//  PostgreSQL
//
//  Created by Andrew J Wagner on 12/10/17.
//

import Foundation
import SQL
import CPostgreSQL

extension SQLError {
    init(connection: OpaquePointer?, message: String? = nil) {
        let moreInformation: String?
        if let info = PQerrorMessage(connection) {
            moreInformation = String(cString: info)
        }
        else {
            moreInformation = nil
        }

        if let message = message {
            self.message = message
            self.moreInformation = moreInformation
        }
        else {
            self.message = moreInformation ?? "Unknown Error"
            self.moreInformation = nil
        }
    }
}

