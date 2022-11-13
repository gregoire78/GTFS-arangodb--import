import { aql } from "arangojs"
import axios from "axios"
import { createReadStream } from "node:fs"
import { readdir, writeFile, access, unlink, rm, rmdir } from "node:fs/promises"
import { Extract } from "unzipper"
import { parse } from "csv-parse"
import path from "node:path"
import { v4 as uuidv4 } from "uuid"
import groupBy from "lodash.groupby"
import { DateTime } from "luxon"
import {
    db,
    Schema,
    Edge,
    DocEdgesImport,
    StopTime,
    Trip,
    Stop,
    Route,
    tripCollection,
    agencyCollection,
    stopCollection,
    usesCollection,
    routesCollection,
    pathwaysCollection,
    calendarCollection,
    locatedAtCollection,
    stopTimesCollection,
    partOfTripCollection,
    calendarDatesCollection,
    partOfStopCollection,
    operatesCollection,
    init,
} from "./init.js"
enum GtfsBool {
    EMPTY = "O",
    YES = "1",
    NO = "2",
}
type NameDefinitions = "agency" | "stop_times" | "trips" | "stops" | "routes" | "calendar" | "calendar_dates" | "pathways"
class Gtfs {
    filename: string
    name: NameDefinitions
    schemas: Schema[] = []
    edges: Edge[] = []
    constructor(filename: string) {
        this.filename = filename
        this.name = <NameDefinitions>filename.replace(".txt", "")
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
                    email: obj.agency_email,
                }
                break
            case "stop_times":
                const key = uuidv4()
                this.schemas.push({
                    _key: key,
                    tripId: obj.trip_id,
                    stopId: obj.stop_id,
                    arrivalTime: obj.arrival_time,
                    departureTime: obj.departure_time,
                    stopSequence: Number(obj.stop_sequence),
                    pickupType: Number(obj.pickup_type),
                    dropOffType: Number(obj.drop_off_type),
                    localZoneId: obj.local_zone_id,
                    stopHeadsign: obj.stop_headsign,
                    timepoint: Number(obj.timepoint),
                })
                this.edges.push({
                    _schema: "part_of_trip",
                    _from: "stop_times/" + key,
                    _to: "trips/" + obj.trip_id,
                })
                this.edges.push({
                    _schema: "located_at",
                    _from: "stop_times/" + key,
                    _to: "stops/" + obj.stop_id,
                })
                break
            case "trips":
                this.schemas.push({
                    _key: obj.trip_id,
                    routeId: obj.route_id,
                    serviceId: obj.service_id,
                    headsign: obj.trip_headsign,
                    shortName: obj.trip_short_name,
                    directionId: Number(obj.direction_id),
                    blockId: obj.block_id,
                    shapeId: obj.shape_id,
                    wheelchairAccessible: this.getBoolWithEmpty(obj.wheelchair_accessible),
                    bikesAllowed: this.getBoolWithEmpty(obj.bikes_allowed),
                })
                this.edges.push({
                    _schema: "uses",
                    _from: "trips/" + obj.trip_id,
                    _to: "routes/" + obj.route_id,
                })
                // this.edges.push({
                //     _schema: "serves",
                //     _from: "calendar/"+obj.service_id,
                //     _to: "trips/"+obj.trip_id
                // })
                // this.edges.push({
                //     _schema: "explicitly_serve",
                //     _from: "calendar_dates/"+obj.service_id,
                //     _to: "trips/"+obj.trip_id
                // })
                break
            case "stops":
                this.schemas.push({
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
                    plateformCode: obj.platform_code,
                })
                if (obj.parent_station && obj.parent_station !== "") {
                    this.edges.push({
                        _schema: "part_of_stop",
                        _from: "stops/" + obj.stop_id,
                        _to: "stops/" + obj.parent_station,
                    })
                }
                break
            case "routes":
                this.schemas.push({
                    _key: obj.route_id,
                    agencyId: obj.agency_id,
                    shortName: obj.route_short_name,
                    longName: obj.route_long_name,
                    desc: obj.route_desc,
                    type: Number(obj.route_type),
                    url: obj.route_url,
                    color: "#" + obj.route_color,
                    textColor: "#" + obj.route_text_color,
                    sortOrder: Number(obj.route_sort_order),
                })
                this.edges.push({
                    _schema: "operates",
                    _from: "agency/" + obj.agency_id,
                    _to: "routes/" + obj.route_id,
                })
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
                    startDate: DateTime.fromFormat(obj.start_date, "yyyyMMdd").toISODate(),
                    endDate: DateTime.fromFormat(obj.end_date, "yyyyMMdd").toISODate(),
                }
                break
            case "calendar_dates":
                schema = {
                    _schema: this.name,
                    serviceId: obj.service_id,
                    date: DateTime.fromFormat(obj.date, "yyyyMMdd").toISODate(),
                    exceptionType: Number(obj.exception_type),
                }
                break
            case "pathways":
                schema = {
                    _schema: this.name,
                    _key: obj.pathway_id,
                    _from: "stops/" + obj.from_stop_id,
                    _to: "stops/" + obj.to_stop_id,
                    mode: Number(obj.pathway_mode),
                    isBidirectionnal: this.getBool(obj.is_bidirectional),
                    length: Number(obj.length),
                    traversalTime: Number(obj.traversal_time),
                    stairCount: Number(obj.stair_count),
                    maxSlope: Number(obj.max_slope),
                    minWidth: Number(obj.min_width),
                    signpostedAs: obj.signposted_as,
                    reversedSignpostedAs: obj.reversed_signposted_as,
                }
                break
            default:
                break
        }
        if (this.name === "stop_times" || this.name === "trips" || this.name === "stops" || this.name === "routes") {
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
                break
        }
    }
    private getBool(o: string) {
        return o === "1"
    }

    private removeEmpty(obj: any) {
        Object.keys(obj).forEach((key) => {
            if (obj[key] === "") {
                delete obj[key]
            }
        })
        return obj
    }

    private async schemaSplice(size: number = 100) {
        if (this.schemas.length >= size) {
            const g = this.schemas.splice(0, size)
            const edges = this.edges.splice(0, size * 2)
            if (g.length > 0 || edges.length > 0) {
                const de = new DocEdgesImport()
                de.docs = g
                de.edges = edges
                return de
            }
        }
    }
}
/**
 * https://prim.iledefrance-mobilites.fr/fr/donnees-statiques/offre-horaires-tc-gtfs-idfm
 */
const gtfsZipUrl =
    "https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/files/a925e164271e4bca93433756d6a340d1/download/"

/**
 * path gtfs folder
 */
const gtfsZipPath = "gtfs.zip"
const gtfsPath = "gtfs"

const downloadFiles = async (url: string, filePath: string) => {
    console.log(`Downloading GTFS from ${url}`)
    const response = await axios(url, { responseType: "arraybuffer" })

    if (response.status !== 200) {
        throw new Error("Couldn’t download files")
    }

    const buffer = Buffer.from(response.data)

    await writeFile(filePath, buffer)
    console.log("Download successful")
}

const importDocsWithEdges = async (gtfsName: NameDefinitions, edges: Edge[], docs: Schema[]) => {
    const e = groupBy(edges, "_schema")
    if (gtfsName === "stop_times") {
        await stopTimesCollection.import(<StopTime[]>docs)
        await partOfTripCollection.import(
            e["part_of_trip"].map((v) => ({
                _from: v._from,
                _to: v._to,
            }))
        )
        await locatedAtCollection.import(
            e["located_at"].map((v) => ({
                _from: v._from,
                _to: v._to,
            }))
        )
    }
    if (gtfsName === "trips") {
        await tripCollection.import(<Trip[]>docs)
        await usesCollection.import(
            e["uses"].map((v) => ({
                _from: v._from,
                _to: v._to,
            }))
        )
        // await servesCollection.import(e["serves"].map(v => ({
        //     _from: v._from,
        //     _to: v._to
        // })))
        // await explicitlyServeCollection.import(e["explicitly_serve"].map(v => ({
        //     _from: v._from,
        //     _to: v._to
        // })))
    }
    if (gtfsName === "stops") {
        await stopCollection.import(<Stop[]>docs)
        await partOfStopCollection.import(
            e["part_of_stop"].map((v) => ({
                _from: v._from,
                _to: v._to,
            }))
        )
    }
    if (gtfsName === "routes") {
        await routesCollection.import(<Route[]>docs)
        await operatesCollection.import(
            e["operates"].map((v) => ({
                _from: v._from,
                _to: v._to,
            }))
        )
    }
}

class GtfsFiles {
    files: Map<NameDefinitions, boolean> = new Map()

    setIsOpen(name: NameDefinitions, isOpen: boolean) {
        if (!isOpen) {
            this.files.delete(name)
            return
        }
        this.files.set(name, isOpen)
    }
}

const readFiles = async (filePath: string) => {
    const textFiles = await getTextFiles(filePath)
    console.log(textFiles)
    const gtfsFiles = new GtfsFiles()
    for (const textFile of textFiles) {
        const gtfs = new Gtfs(textFile)
        const filepath = path.join(filePath, `${textFile}`)
        const parser = parse({
            columns: true,
            bom: true,
            trim: true,
            skipRecordsWithError: true,
            skipEmptyLines: true,
        })
        parser.on("readable", async () => {
            let record
            while ((record = parser.read()) !== null) {
                // Work with each record
                const data = await gtfs.add(record)
                if (data instanceof DocEdgesImport) {
                    console.log(gtfs.name, data.docs.length, data.edges.length)
                    await importDocsWithEdges(gtfs.name, data.edges, data.docs)
                    continue
                }
                if (data) {
                    const schema = data._schema
                    delete data._schema
                    switch (schema) {
                        case "agency":
                            await agencyCollection.save(data)
                            break
                        case "calendar":
                            await calendarCollection.save(data)
                            break
                        case "calendar_dates":
                            await calendarDatesCollection.save(data)
                            break
                        case "pathways":
                            await pathwaysCollection.save(data)
                            break
                        default:
                            break
                    }
                }
            }
        })
        parser.on("end", async () => {
            console.log(gtfs.name)
            await importDocsWithEdges(gtfs.name, gtfs.edges, gtfs.schemas)
        })

        gtfsFiles.setIsOpen(gtfs.name, true)
        const stream = createReadStream(filepath).pipe(parser)
        stream.on("close", async () => {
            gtfsFiles.setIsOpen(gtfs.name, false)
            if (gtfs.name === "stop_times") {
                await createEdgesPrecedes()
            }
            if (gtfs.name === "calendar" && !gtfsFiles.files.has("trips")) {
                await createEdgesServes()
            }
            if (gtfs.name === "trips" && !gtfsFiles.files.has("agency")) {
                await createEdgesServes()
            }
            if (gtfsFiles.files.size === 0) {
                await createEdgesHasRoute()
            }
        })
    }
}

export const createEdgesPrecedes = async () => {
    console.log("edge precedes")
    try {
        await db.query(
            aql`
        for t in trips
        for s1 in 1 inbound t._id part_of_trip
        for s2 in 1 inbound concat("trips/",s1.tripId) part_of_trip
            filter s2.stopSequence == s1.stopSequence+1
            insert { _from: s1._id, _to: s2._id } into precedes options { ignoreErrors: true }
        `,
            { intermediateCommitCount: 100_000 }
        )
    } catch (error: any) {
        console.log(error.message)
    }
    // const trx = await db.beginTransaction({ write: [precedesCollection], read: [tripCollection, stopTimesCollection] })
    // await trx.step(() => precedesCollection.save({
    //     _from: left._id,
    //     _to: right._id,
    //     data: "potato"
    // }))
}

export const createEdgesServes = async () => {
    console.log("edge serves")
    try {
        await db.query(
            aql`
        for c in calendar
        for t in trips
            filter t.serviceId == c.serviceId
            insert { _from: c._id, _to: t._id } into serves options { ignoreErrors: true }
        `,
            { intermediateCommitCount: 100000 }
        )
    } catch (error: any) {
        console.log(error.message)
    }
}

export const createEdgesHasRoute = async () => {
    console.log("edge has routes")
    try {
        await db.query(
            aql`
        for loc in stops
            let routes = unique(
                for v, e, p in 3 inbound loc._id located_at, outbound part_of_trip, outbound uses
                    let route = p.vertices[3]
                    return route
            )
            for r in routes
                insert { _from: loc._id, _to: r._id } into has_routes options { ignoreErrors: true }
        `,
            { intermediateCommitCount: 100000 }
        )
    } catch (error: any) {
        console.log(error.message)
    }
}

/**
 * check if can acces to file/folder
 * @param path string
 * @returns void
 */
export async function exist(filePath: string) {
    try {
        await access(filePath)
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
    return files.filter((filename) => filename.slice(-3) === "txt")
}

/*
 * Unzip a zipfile into a specified directory
 */
export function unzip(zipfilePath: string, exportPath: string) {
    return createReadStream(zipfilePath)
        .pipe(Extract({ path: exportPath }))
        .on("entry", (entry) => entry.autodrain())
        .promise()
}
if (!(await exist(gtfsPath))) {
    if (!(await exist(gtfsZipPath))) {
        await downloadFiles(gtfsZipUrl, gtfsZipPath)
    }
    await unzip(gtfsZipPath, gtfsPath)
    await unlink(gtfsZipPath)
}
await init()
await readFiles(gtfsPath)
// await rm(gtfsPath, { recursive: true, force: true })
await rmdir(gtfsPath, { recursive: true })
