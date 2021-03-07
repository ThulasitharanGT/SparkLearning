package org.controller.explorations.tmpExploration

import java.nio.file.{Path,Paths}
object javaNIOPath {
def main(args:Array[String]):Unit={
  val paths=Paths.get("/home/raptor/Softwares/idea-IC-203.7148.57")
  paths.toAbsolutePath
  paths.getNameCount // number of files in that directory
  paths.getFileName // folder or file name
  paths.subpath(0,2) // like a substring for the dirs
  paths.subpath(0,2).getNameCount
  val paths2=Paths.get("/home/raptor","/Softwares/idea-IC-203.7148.57") // this is reative path
  Paths.get("/home/raptor/../Softwares/idea-IC-203.7148.57").getFileName // gets parent dir of sofwates, ie the current directory of raptor
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").getFileName// gets current dir of softwares
  Paths.get("/home/raptor/","./Softwares/idea-IC-203.7148.57") // same as usual path
  Paths.get("/home/raptor/","../tmp/Softwares/idea-IC-203.7148.57")

  val tmpPath1=Paths.get("/home/raptor/")
  val tmpPath2=Paths.get("/Softwares/idea-IC-203.7148.57")
  val tmpPath3=Paths.get("/home/raptor/Softwares/idea-IC-203.7148.57")
  val tmpPath4=Paths.get("tmp.txt")
  tmpPath1.relativize(tmpPath2)
  tmpPath1.relativize(tmpPath3) // subtracts common path and gives the remaining
  tmpPath1.relativize(tmpPath4)
  Paths.get("/home/raptor/tmp/","../Softwares/idea-IC-203.7148.57").normalize
  Paths.get("/home/raptor/tmp/../Softwares/idea-IC-203.7148.57").normalize
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").normalize
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").subpath(0,2)
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").getParent
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").getRoot
  Paths.get("/home/raptor/","/Softwares/idea-IC-203.7148.57").toRealPath()
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").toFile
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").compareTo(Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57")) // same so zero
  Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57").compareTo(Paths.get("/home/raptor/")) // deep with root ,so in positive
  Paths.get("/home/raptor/").compareTo(Paths.get("/home/raptor/./Softwares/idea-IC-203.7148.57")) // root with deep so its in negative
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith(Paths.get("/home/raptor/","Softwares")) //false
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith(Paths.get("/home/raptor/","Softwares/","idea-IC-203.7148.57")) //true
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith(Paths.get("/home/raptor/",/*"Softwares/",*/"idea-IC-203.7148.57")) // false
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith(Paths.get("/home/","Softwares/idea-IC-203.7148.57")) // false
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith("Softwares/idea-IC-203.7148.57") // true
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith("idea-IC-203.7148.57") // true
  Paths.get("/home/raptor/","Softwares/idea-IC-203.7148.57").endsWith("tmp") // false



}
}
