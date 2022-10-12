import { Database } from "arangojs"
import axios from "axios"
import { createReadStream } from "node:fs"
import { readdir, writeFile, access } from 'node:fs/promises'
import { Extract } from 'unzipper'
import { parse } from 'csv-parse'
import path from "node:path"

interface Agency {
    _schema?: "agency"
    _key: string,
    name: string
    url: string
    timezone: string
    lang?: string
    phone?: string
    email?: string
}

interface StopTime {
    _schema?: "stop_times"
    _from?: string
    arrivalTime?: string
    departureTime?: string
    _to?: string
    stopSequence?: string
    pickupType?: PickupType | null
    dropOffType?: PickupType | null
    localZoneId?: string
    stopHeadsign?: string
    timepoint?: Timepoint
}

enum Timepoint {
    APPROXIMATE = 0,
    EXACT = 1
}

enum GtfsBool {
    EMPTY = "O",
    YES = "1",
    NO = "2"
}

interface Trip {
    _schema?: "trips"
    _key: string
    routeId: string
    serviceId: string
    headsign?: string
    shortName?: string
    // /**
    //  * - "0" Outbound
    //  * - "1" Inbound
    //  */
    // directionId?: "0" | "1"
    directionId?: DirectionId
    blockId?: string
    shapeId?: string
    wheelchairAccessible?: boolean | null
    bikesAllowed?: boolean | null
}

interface Stop {
    _schema?: "stops"
    _key: string
    code?: string
    name?: string
    desc?: string
    lon?: number
    lat?: number
    zoneId?: string
    url?: string
    // /**
    //  * - null or "0" Stop or Platform
    //  * - "1" Station
    //  * - "2" Entrance/Exit
    //  * - "3" Generic Node
    //  * - "4" Boarding Area
    //  */
    // locationType?:  "0" | "1" | "2" | "3" | "4" | null
    locationType?:  LocationType | null
    parentStation?: string
    timezone?: string
    levelId?: string
    wheelchairBoarding?: boolean | null
    plateformCode?: string
}

enum LocationType {
    STOP = 0,
    STATION = 1,
    ENTRANCE_OR_EXIT = 2,
    GENERIC_NODE = 3,
    BOARDING_AREA = 4
}

enum RouteType {
    /**
     * Tram, Streetcar, Light rail. Any light rail or street level system within a metropolitan area.
     */
    TRAM = 0,
    /**
     * Subway, Metro. Any underground rail system within a metropolitan area.
     */
    METRO = 1,
    /**
     * Rail. Used for intercity or long-distance travel.
     */
    RAIL = 2,
    /**
     * Bus. Used for short- and long-distance bus routes.
     */
    BUS = 3,
    /**
     * Ferry. Used for short- and long-distance boat service.
     */
    FERRY = 4,
    /**
     * Cable tram. Used for street-level rail cars where the cable runs beneath the vehicle, e.g., cable car in San Francisco.
     */
    CABLE_TRAM = 5,
    /**
     * Aerial lift, suspended cable car (e.g., gondola lift, aerial tramway). Cable transport where cabins, cars, gondolas or open chairs are suspended by means of one or more cables.
     */
    AERIAL_LIFT = 6,
    /**
     * Funicular. Any rail system designed for steep inclines.
     */
    FUNICULAR = 7,
    /**
     * Trolleybus. Electric buses that draw power from overhead wires using poles.
     */
    TROLLEYBUS = 11,
    /**
     * Monorail. Railway in which the track consists of a single rail or a beam.
     */
    MONORAIL = 12,
}

enum DirectionId {
    OUTBOUND = 0,
    INBOUND = 1
}

enum PickupType {
    /**
     * Regularly scheduled pickup.
     */
    YES = 0,
    /**
     * No pickup.
     */
    NO = 1,
    /**
     * Must phone agency to arrange pickup.
     */
    PHOME_AGENCY = 2,
    /**
     * Must coordinate with driver to arrange pickup.
     */
    ASK_DRIVER = 3
}

enum ExceptionType {
    /**
     * Service has been added for the specified date.
     */
    ADDED = 1,
    /**
     * Service has been removed for the specified date.
     */
    REMOVED = 2
}

enum PathwayMode {
    WALKWAY = 1,
    STAIRS = 2,
    TRAVELATOR = 3,
    ESCALATOR = 4,
    ELEVATOR = 5,
    FARE_GATE = 6,
    EXIT_GATE = 7
}

interface Route {
    _schema?: "routes"
    _key: string
    agencyId?: string
    shortName?: string
    longName?: string
    desc?: string
    type: RouteType
    url?: string
    color?: string
    textColor?: string
    sortOrder?: number
}

interface Calendar {
    _schema?: "calendar"
    serviceId: string
    monday: boolean
    tuesday: boolean
    wednesday: boolean
    thursday: boolean
    friday: boolean
    saturday: boolean
    sunday: boolean
    startDate: string
    endDate: string
}

interface CalendarDates {
    _schema?: "calendar_dates"
    serviceId: string
    date: string
    exceptionType: ExceptionType
}

interface Pathway {
    _schema?: "pathways"
    _key: string
    _from: string
    _to: string
    mode: PathwayMode,
    isBidirectionnal: boolean,
    length?: number,
    traversalTime?: number,
    stairCount?: number,
    maxSlope?: number,
    minWidth?: number,
    signpostedAs?: string,
    reversedSignpostedAs?: string
}

type Schema = Agency | StopTime | Trip | Stop | Route | Calendar | CalendarDates | Pathway

const db = new Database({
    url: "http://localhost:8529",
    databaseName: "GTFS"
})
const agencyCollection = db.collection<Agency>("agency")
const tripCollection = db.collection<Trip>("trips")
const stopCollection = db.collection<Stop>("stops")
await stopCollection.ensureIndex({ type: "geo", fields: [ "lat", "lon" ] })
const stopTimesCollection = db.collection<StopTime>("stop_times")
await stopTimesCollection.truncate()
const routesCollection = db.collection<Route>("routes")
const calendarCollection = db.collection<Calendar>("calendar")
await calendarCollection.ensureIndex({ type: "persistent", name: "service", fields: ["serviceId"] })
await calendarCollection.truncate()
const calendarDatesCollection = db.collection<CalendarDates>("calendar_dates")
await calendarDatesCollection.ensureIndex({ type: "persistent", name: "service", fields: ["serviceId"] })
await calendarDatesCollection.truncate()
const pathwaysCollection = db.collection<Pathway>("pathways")

type NameDefinitions = "agency" | "stop_times" | "trips" | "stops" | "routes" | "calendar" | "calendar_dates" | "pathways"
class Gtfs {
    filename: string
    name: NameDefinitions
    schemas: Schema[] = []
    constructor(filename: string) {
        this.filename = filename
        this.name = <NameDefinitions>filename.replace('.txt', '')
    }

    async add(obj: any) {
        this.removeEmpty(obj)
        let schema: Schema | null = null
        switch (this.name) {
            case "agency":
                schema = {
                    _schema: this.name,
                    _key: obj.agency_id,
                    name: obj.agency_name,
                    url: obj.agency_url,
                    timezone: obj.agency_timezone,
                    lang: obj.agency_lang,
                    phone: obj.agency_phone,
                    email: obj.agency_email
                }
                break
            case "stop_times":
                schema = {
                    _schema: this.name,
                    _from: "trips/"+obj.trip_id,
                    arrivalTime: obj.arrival_time,
                    departureTime: obj.departure_time,
                    _to: "stops/"+obj.stop_id,
                    stopSequence: obj.stop_sequence,
                    pickupType: Number(obj.pickup_type),
                    dropOffType: Number(obj.drop_off_type),
                    localZoneId: obj.local_zone_id,
                    stopHeadsign: obj.stop_headsign,
                    timepoint: Number(obj.timepoint)
                }
                break
            case "trips":
                schema = {
                    _schema: this.name,
                    _key: obj.trip_id,
                    routeId: obj.route_id,
                    serviceId: obj.service_id,
                    headsign: obj.trip_headsign,
                    shortName: obj.trip_short_name,
                    directionId: Number(obj.direction_id),
                    blockId: obj.block_id,
                    shapeId: obj.shape_id,
                    wheelchairAccessible: this.getBoolWithEmpty(obj.wheelchair_accessible),
                    bikesAllowed: this.getBoolWithEmpty(obj.bikes_allowed)
                }
                break
            case "stops":
                schema = {
                    _schema: this.name,
                    _key: obj.stop_id,
                    code: obj.stop_code,
                    name: obj.stop_name,
                    desc: obj.stop_desc,
                    lon: Number(obj.stop_lon),
                    lat: Number(obj.stop_lat),
                    zoneId: obj.zone_id,
                    url: obj.stop_url,
                    locationType: Number(obj.location_type),
                    parentStation: obj.parent_station,
                    timezone: obj.stop_timezone,
                    levelId: obj.level_id,
                    wheelchairBoarding: this.getBoolWithEmpty(obj.wheelchair_boarding),
                    plateformCode: obj.platform_code
                }
                break
            case "routes":
                schema = {
                    _schema: this.name,
                    _key: obj.route_id,
                    agencyId: obj.agency_id,
                    shortName: obj.route_short_name,
                    longName: obj.route_long_name,
                    desc: obj.route_desc,
                    type: Number(obj.route_type),
                    url: obj.route_url,
                    color: "#"+obj.route_color,
                    textColor: "#"+obj.route_text_color,
                    sortOrder: Number(obj.route_sort_order)
                }
                break
            case "calendar":
                schema = {
                    _schema: this.name,
                    serviceId: obj.service_id,
                    monday: this.getBool(obj.monday),
                    tuesday: this.getBool(obj.tuesday),
                    wednesday: this.getBool(obj.wednesday),
                    thursday: this.getBool(obj.thursday),
                    friday: this.getBool(obj.friday),
                    saturday: this.getBool(obj.saturday),
                    sunday: this.getBool(obj.sunday),
                    startDate: obj.start_date,
                    endDate: obj.end_date
                }
                break
            case "calendar_dates":
                schema = {
                    _schema: this.name,
                    serviceId: obj.service_id,
                    date: obj.date,
                    exceptionType: Number(obj.exception_type)
                }
                break
            case "pathways":
                schema = {
                    _schema: this.name,
                    _key: obj.pathway_id,
                    _from: "stops/"+obj.from_stop_id,
                    _to: "stops/"+obj.to_stop_id,
                    mode: Number(obj.pathway_mode),
                    isBidirectionnal: this.getBool(obj.is_bidirectional),
                    length: Number(obj.length),
                    traversalTime: Number(obj.traversal_time),
                    stairCount: Number(obj.stair_count),
                    maxSlope: Number(obj.max_slope),
                    minWidth: Number(obj.min_width),
                    signpostedAs: obj.signposted_as,
                    reversedSignpostedAs: obj.reversed_signposted_as
                }
                break
            default: break
        }
        if(this.name === "stop_times") {
            this.schemas.push(<StopTime>schema)
            return this.schemaSplice(50000)
        }
        return schema
    }

    private getBoolWithEmpty(o: string) {
        switch (o) {
            case GtfsBool.EMPTY:
                return null
            case GtfsBool.YES:
                return true
            case GtfsBool.NO:
                return false
            default:
                break;
        }
    }
    private getBool(o: string) {
        return o === "1"
    }

    private removeEmpty(obj: any) {
        Object.keys(obj).forEach(key => {
            if (obj[key] === '') {
              delete obj[key];
            }
        })
        return obj
    }

    private async schemaSplice(size: number = 100) {
        if(this.schemas.length >= size) {
            const g = this.schemas.splice(0, size)
            if(g.length > 0) {
                return g
            }
        }
    }
}
/**
 * https://prim.iledefrance-mobilites.fr/fr/donnees-statiques/offre-horaires-tc-gtfs-idfm
 */
const gtfsZipUrl = "https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/files/a925e164271e4bca93433756d6a340d1/download/"

/**
 * path gtfs folder
 */
const gtfsZipPath = 'gtfs.zip'
const gtfsPath = 'gtfs'

const downloadFiles = async (url: string, gtfsPath: string) => {
    console.log(`Downloading GTFS from ${url}`)
    const response = await axios(url, {responseType: "arraybuffer"})

    if (response.status !== 200) {
        throw new Error('Couldn’t download files')
    }

    const buffer = Buffer.from(response.data)

    await writeFile(gtfsPath, buffer)
    console.log('Download successful')
}

const readFiles = async (gtfsPath: string) => {
    const textFiles = await getTextFiles(gtfsPath)
    console.log(textFiles)

    for (const textFile of textFiles) {
        const gtfs = new Gtfs(textFile)
        const filepath = path.join(gtfsPath,`${textFile}`);
        const parser = parse({
            columns: true,
            bom: true,
            trim: true,
            skipRecordsWithError: true,
            skipEmptyLines: true,
        })
        parser.on("readable", async () => {
            let record; while ((record = parser.read()) !== null) {
                // Work with each record
                const data = await gtfs.add(record)
                if(Array.isArray(data)) {
                    console.log(gtfs.name, data.length)
                    await stopTimesCollection.import(<StopTime[]>data)
                    continue
                }
                const overwriteMode = "ignore"
                if(data) {
                    const schema = data._schema
                    delete data._schema
                    switch (schema) {
                        case "agency":
                            await agencyCollection.save(data, {
                                overwriteMode
                            })
                            break
                        case "trips":
                            await tripCollection.save(data, {
                                overwriteMode
                            })
                            break
                        case "stops":
                            await stopCollection.save(data, {
                                overwriteMode
                            })
                            break
                        case "stop_times":
                            break
                        case "routes":
                            await routesCollection.save(data, {
                                overwriteMode
                            })
                            break
                        case "calendar":
                            await calendarCollection.save(data)
                            break
                        case "calendar_dates":
                            await calendarDatesCollection.save(data)
                            break
                        case "pathways":
                            await pathwaysCollection.save(data, {
                                overwriteMode
                            })
                            break
                        default: break
                    }
                }
            }
        })
        parser.on("end", async () => {
            console.log(gtfs.name)
            const data =  gtfs.schemas
            if(gtfs.name === "stop_times") {
                await stopTimesCollection.import(<StopTime[]>data)
            }
        })
        // const transformer = transform({
        //     parallel: 5
        // }, (record, cb) => {
        //     setTimeout(() => {
        //         cb(null, record+'\n');
        //     }, 500)
        // })
        createReadStream(filepath).pipe(parser)
    }
}

/**
 * check if can acces to file/folder
 * @param path string
 * @returns void
 */
export async function exist(path: string) {
    try {
        await access(path)
        return true
    } catch {
        return false
    }
}

/**
 * get texts files in gtfs folder
 * @param folderPath 
 * @returns 
 */
export async function getTextFiles(folderPath: string) {
    const files = await readdir(folderPath)
    return files.filter((filename) => filename.slice(-3) === 'txt')
}

/*
 * Unzip a zipfile into a specified directory
 */
export function unzip(zipfilePath: string, exportPath: string) {
    return createReadStream(zipfilePath)
      .pipe(Extract({ path: exportPath }))
      .on('entry', (entry) => entry.autodrain())
      .promise()
}

if (!await exist(gtfsZipPath)){
    await downloadFiles(gtfsZipUrl, gtfsZipPath)
}
if (!await exist(gtfsPath)){
    await unzip(gtfsZipPath, gtfsPath)
}
await readFiles(gtfsPath)