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
            self.init(message: message, moreInformation: moreInformation)
        }
        else {
            self.init(message: moreInformation ?? "Unknown Error", moreInformation: nil)
        }
    }
}

