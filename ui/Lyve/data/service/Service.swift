//
//  network.swift
//  Lyve
//
//  Created by Josh Leung on 5/26/20.
//  Copyright © 2020 Josh Leung. All rights reserved.
//

import Foundation

class Service {
    
    func TestGet() {
        var components = URLComponents()
        components.host = "https://covidtracking.com/api"
        components.scheme = "v1/us/current.json"
    }
    
}
