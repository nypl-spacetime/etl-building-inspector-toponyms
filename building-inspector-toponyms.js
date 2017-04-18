'use strict'

const fs = require('fs')
const path = require('path')
const R = require('ramda')
const H = require('highland')
const IndexedGeo = require('indexed-geo')

function toFeature (object) {
  if (object.geometry) {
    return {
      type: 'Feature',
      properties: R.omit(['geometry'], object),
      geometry: object.geometry
    }
  }
}

function isBuildingPolygon (object) {
  return object.type === 'st:Building' && object.geometry && object.geometry.type === 'Polygon'
}

function isBuildingToponym (object) {
  return object.type === 'st:Building' && object.geometry && object.geometry.type === 'Point'
}

function getBuildingInspectorObjects (dirs) {
  const baseDir = path.join(dirs.current, '..')
  const objectsFile = path.join(baseDir, 'building-inspector', 'building-inspector.objects.ndjson')

  return H(fs.createReadStream(objectsFile))
    .split()
    .compact()
    .map(JSON.parse)
}

function transform (config, dirs, tools, callback) {
  let indices = {}

  getBuildingInspectorObjects(dirs)
    .filter(isBuildingPolygon)
    .group((object) => object.data.layerId)
    .map(R.toPairs)
    .sequence()
    .each((pair) => {
      const layerId = pair[0]

      const geojson = {
        type: 'FeatureCollection',
        features: pair[1].map(toFeature)
      }

      let indexedGeo = IndexedGeo()
      indexedGeo.index(geojson)

      indices[layerId] = indexedGeo
    })
    .done(() => {
      console.log('      Done indexing geometries')

      getBuildingInspectorObjects(dirs)
        .filter(isBuildingToponym)
        .map((toponym) => {
          const layerId = toponym.data.layerId
          const indexedGeo = indices[layerId]

          if (!indexedGeo) {
            return new Error(`No geospatial index found for map layer ${layerId}`)
          }

          let buildings = []
          try {
            buildings = indexedGeo.inside(toponym.geometry)
          } catch (err) {
            // TODO: log errors
          }

          if (buildings.length) {
            return buildings.map((building) => ({
              type: 'relation',
              obj: {
                from: `building-inspector/${toponym.id}`,
                to: `building-inspector/${building.properties.id}`,
                type: 'st:sameAs'
              }
            }))
          } else {
            // TODO: log errors
          }
        })
        .compact()
        .flatten()
        .map(H.curry(tools.writer.writeObject))
        .nfcall([])
        .series()
        .errors(callback)
        .done(callback)
    })
}

// ==================================== API ====================================

module.exports.steps = [
  transform
]
