//
//  PostgreSQLConnectionTests.swift
//  PostgreSQLTests
//
//  Created by Andrew J Wagner on 12/4/18.
//

import XCTest
@testable import PostgreSQL

class PostgreSQLTests: XCTestCase {
    func testBasicReplaceParametersWithNumbers() {
        let connection = PostgreSQLConnection(host: "host", databaseName: "test_db", username: "username", password: "password")
        let input = "SELECT * FROM table WHERE columnA = %@ AND columnB = %@ AND function(%@) = 'result'"
        let output = connection.replaceParametersWithNumbers(in: input)
        XCTAssertEqual(output, "SELECT * FROM table WHERE columnA = $1 AND columnB = $2 AND function($3) = 'result'")
    }

    func testBasicReplaceParametersWithToTimestamp() {
        let connection = PostgreSQLConnection(host: "host", databaseName: "test_db", username: "username", password: "password")
        let input = "SELECT * FROM table WHERE columnA = %@ AND columnB = ====to_timestamp====(%@) AND ====to_timestamp====(function(%@)) = ====to_timestamp====( function('result') )"
        let output = connection.replaceParametersWithNumbers(in: input)
        XCTAssertEqual(output, "SELECT * FROM table WHERE columnA = $1 AND columnB = to_timestamp($2,'YYYY-MM-DD HH24:MI:SS.USZ') AND to_timestamp(function($3),'YYYY-MM-DD HH24:MI:SS.USZ') = to_timestamp( function('result') ,'YYYY-MM-DD HH24:MI:SS.USZ')")
    }

    func testBasicReplaceParametersWithToLocalTimestamp() {
        let connection = PostgreSQLConnection(host: "host", databaseName: "test_db", username: "username", password: "password")
        let input = "SELECT * FROM table WHERE columnA = %@ AND columnB = ====to_local_timestamp====(%@) AND ====to_local_timestamp====(function(%@)) = ====to_local_timestamp====( function('result') )"
        let output = connection.replaceParametersWithNumbers(in: input)
        XCTAssertEqual(output, "SELECT * FROM table WHERE columnA = $1 AND columnB = to_timestamp($2,'YYYY-MM-DD HH24:MI:SS.US') AND to_timestamp(function($3),'YYYY-MM-DD HH24:MI:SS.US') = to_timestamp( function('result') ,'YYYY-MM-DD HH24:MI:SS.US')")
    }

    static var allTests = [
        ("testBasicReplaceParametersWithNumbers", testBasicReplaceParametersWithNumbers),
        ("testBasicReplaceParametersWithToTimestamp", testBasicReplaceParametersWithToTimestamp),
        ("testBasicReplaceParametersWithToLocalTimestamp", testBasicReplaceParametersWithToLocalTimestamp),
    ]
}

