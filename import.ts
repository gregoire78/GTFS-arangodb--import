import { unlink, rm, rmdir } from "node:fs/promises"
import { init } from "./init.js"
import { downloadFiles, exist, readFiles, unzip } from "./main.js"

/**
 * https://prim.iledefrance-mobilites.fr/fr/donnees-statiques/offre-horaires-tc-gtfs-idfm
 */
// const gtfsZipUrl =
//     "https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm/files/a925e164271e4bca93433756d6a340d1/download/"
const gtfsZipUrl = "https://exs.breizgo.cityway.fr/ftp/GTFS/MOBIBREIZHBRET.gtfs.zip"

/**
 * path gtfs folder
 */
const gtfsZipPath = "gtfs.zip"
const gtfsPath = "gtfs"

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
