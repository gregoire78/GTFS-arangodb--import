import { Database } from "arangojs"
import { CollectionType, DocumentCollection, EdgeCollection } from "arangojs/collection.js"
const { EDGE_COLLECTION, DOCUMENT_COLLECTION } = CollectionType

export interface Agency {
    _schema?: "agency"
    _key: string
    name: string
    url: string
    timezone: string
    lang?: string
    phone?: string
    email?: string
}

export enum Timepoint {
    APPROXIMATE = 0,
    EXACT = 1,
}

export enum LocationType {
    STOP = 0,
    STATION = 1,
    ENTRANCE_OR_EXIT = 2,
    GENERIC_NODE = 3,
    BOARDING_AREA = 4,
}

export enum RouteType {
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

export enum DirectionId {
    OUTBOUND = 0,
    INBOUND = 1,
}

export enum PickupType {
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
    ASK_DRIVER = 3,
}

export enum ExceptionType {
    /**
     * Service has been added for the specified date.
     */
    ADDED = 1,
    /**
     * Service has been removed for the specified date.
     */
    REMOVED = 2,
}

export enum PathwayMode {
    WALKWAY = 1,
    STAIRS = 2,
    TRAVELATOR = 3,
    ESCALATOR = 4,
    ELEVATOR = 5,
    FARE_GATE = 6,
    EXIT_GATE = 7,
}

export interface StopTime {
    _schema?: "stop_times"
    _key: string
    tripId?: string
    arrivalTime?: string
    departureTime?: string
    stopId?: string
    stopSequence?: number
    pickupType?: PickupType | null
    dropOffType?: PickupType | null
    localZoneId?: string
    stopHeadsign?: string
    timepoint?: Timepoint
}

export interface Trip {
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

export interface Stop {
    _schema?: "stops"
    _key: string
    code?: string
    name?: string
    desc?: string
    lon?: number
    lat?: number
    zoneId?: string
    url?: string
    // locationType?:  "0" | "1" | "2" | "3" | "4" | null
    /**
     * - null or "0" Stop or Platform
     * - "1" Station
     * - "2" Entrance/Exit
     * - "3" Generic Node
     * - "4" Boarding Area
     */
    locationType?: LocationType | null
    parentStation?: string
    timezone?: string
    levelId?: string
    wheelchairBoarding?: boolean | null
    plateformCode?: string
}

export interface Route {
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

export interface Calendar {
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

export interface CalendarDates {
    _schema?: "calendar_dates"
    serviceId: string
    date: string
    exceptionType: ExceptionType
}
//edges
export interface Pathway {
    _schema?: "pathways"
    _key: string
    _from: string
    _to: string
    mode: PathwayMode
    isBidirectionnal: boolean
    length?: number
    traversalTime?: number
    stairCount?: number
    maxSlope?: number
    minWidth?: number
    signpostedAs?: string
    reversedSignpostedAs?: string
}
export interface Edge {
    _schema?: "part_of_trip" | "located_at" | "uses" | "part_of_stop" | "operates" | "serves" | "explicitly_serve"
    _from: string
    _to: string
}
export type Documents = Agency | StopTime | Trip | Stop | Route | Calendar | CalendarDates
export type Schema = Documents | Pathway
export class DocEdgesImport {
    docs: Schema[] = []
    edges: Edge[] = []
}

const createDb = async (dbName: string) => {
    const db = new Database({
        url: "http://localhost:8529",
    })
    if (!(await db.database(dbName).exists())) {
        return db.createDatabase(dbName, [
            {
                username: "root",
            },
        ])
    }
    return db.database(dbName)
}
export const db = await createDb("GTFS")

export const agencyCollection = db.collection<Agency>("agency")
export const tripCollection = db.collection<Trip>("trips")
export const stopCollection = db.collection<Stop>("stops")
export const stopTimesCollection = db.collection<StopTime>("stop_times")
export const routesCollection = db.collection<Route>("routes")
export const calendarCollection = db.collection<Calendar>("calendar")
export const calendarDatesCollection = db.collection<CalendarDates>("calendar_dates")
//edges
export const pathwaysCollection = db.collection<Pathway>("pathways")
export const partOfTripCollection = db.collection<Edge>("part_of_trip")
export const partOfStopCollection = db.collection<Edge>("part_of_stop")
export const locatedAtCollection = db.collection<Edge>("located_at")
export const usesCollection = db.collection<Edge>("uses")
export const precedesCollection = db.collection<Edge>("precedes")
export const operatesCollection = db.collection<Edge>("operates")
export const servesCollection = db.collection<Edge>("serves")
export const hasRoutesCollection = db.collection<Edge>("has_routes")
// export const explicitlyServeCollection = db.collection<Edge>("explicitly_serve")

const truncOrCreate = async (col: DocumentCollection & EdgeCollection, type: CollectionType = DOCUMENT_COLLECTION) => {
    ;(await col.exists())
        ? await col.truncate()
        : await col.create({
              type,
          })
}

export const init = async () => {
    await truncOrCreate(agencyCollection)
    await truncOrCreate(tripCollection)
    await truncOrCreate(stopCollection)
    await truncOrCreate(stopTimesCollection)
    await truncOrCreate(routesCollection)
    await truncOrCreate(calendarCollection)
    await truncOrCreate(calendarDatesCollection)
    await truncOrCreate(pathwaysCollection, EDGE_COLLECTION)
    await truncOrCreate(partOfTripCollection, EDGE_COLLECTION)
    await truncOrCreate(partOfStopCollection, EDGE_COLLECTION)
    await truncOrCreate(locatedAtCollection, EDGE_COLLECTION)
    await truncOrCreate(usesCollection, EDGE_COLLECTION)
    await truncOrCreate(precedesCollection, EDGE_COLLECTION)
    await truncOrCreate(operatesCollection, EDGE_COLLECTION)
    await truncOrCreate(servesCollection, EDGE_COLLECTION)
    await truncOrCreate(hasRoutesCollection, EDGE_COLLECTION)
    // await truncOrCreate(explicitlyServeCollection, EDGE_COLLECTION)

    await calendarDatesCollection.ensureIndex({
        type: "persistent",
        name: "service",
        fields: ["serviceId"],
    })
    await stopCollection.ensureIndex({ type: "geo", fields: ["lat", "lon"] })
    await calendarCollection.ensureIndex({
        type: "persistent",
        name: "service",
        fields: ["serviceId"],
    })
    await precedesCollection.ensureIndex({
        type: "persistent",
        name: "fromto",
        fields: ["_from", "_to"],
        unique: true,
    })
    await partOfStopCollection.ensureIndex({
        type: "persistent",
        name: "fromto",
        fields: ["_from", "_to"],
        unique: true,
    })
    await operatesCollection.ensureIndex({
        type: "persistent",
        name: "fromto",
        fields: ["_from", "_to"],
        unique: true,
    })
    await servesCollection.ensureIndex({
        type: "persistent",
        name: "fromto",
        fields: ["_from", "_to"],
        unique: true,
    })
    await hasRoutesCollection.ensureIndex({
        type: "persistent",
        name: "fromto",
        fields: ["_from", "_to"],
        unique: true,
    })
    // await explicitlyServeCollection.ensureIndex({ type: "persistent", name: "fromto", fields: ["_from", "_to"], unique: true })
}
