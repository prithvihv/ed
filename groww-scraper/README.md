# groww-scraper

FIXME: description

## Installation

Download from http://example.com/FIXME.

## Usage

FIXME: explanation

    $ java -jar groww-scraper-0.1.0-standalone.jar [args]

## Options

FIXME: listing of options this app accepts.

## Examples

...

### Bugs

...

### Any Other Sections
### That You Think
### Might be Useful
- if you can't drop database `ed` it's probably because Grafana is still connected to it.

#### MF transactions

1. dashboard data
https://groww.in/v1/api/portfolio/v2/dashboard?actTime=<time?>&list_tracked=true

2. for mf related info
https://groww.in/v1/api/data/mf/web/v2/scheme/search/<search_id>

3. for every dasboard object call:
https://groww.in/v1/api/portfolio/v1/transaction/scheme/all?folio_number=<folio_number>&page=<pg-no>&scheme_code=<scheme_code>&size=50


#### explore later
https://netsage-project.github.io/gdg/docs/usage_guide/

## License

Copyright © 2022 FIXME

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
