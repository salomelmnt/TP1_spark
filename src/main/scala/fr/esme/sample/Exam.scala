package fr.esme.sample

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.io.StdIn.readLine


object Exam{
  def main(args: Array[String]): Unit = {
      import org.apache.spark.sql.functions._

      Logger.getLogger("org").setLevel(Level.OFF)

      val sparkSession = SparkSession.builder().master("local").getOrCreate()


      //            ######### EXERCICE 1 ##########

      val film = sparkSession.sparkContext.textFile("data/donnees.csv")
      film.foreach(println)


      // 2 Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?

      val acteur = film.map((elem: String) => elem.split(";")(3))
      val LDC = acteur.filter(elem => elem.contains("Di Caprio"))
      println(LDC.count())


      //3 Quelle est la moyenne des notes des films de Di Caprio ?

      val dicaprio = film.map(elem => elem.split(";")(2).toDouble)
      println(dicaprio.mean())



      //4. Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
      val vueDCP = film.map(elem => elem.split(";")(1).toDouble)
      val vuetot= film.map((elem: String) => elem.split(";")(1))
      val totDCP = vueDCP.reduce((a,b)=> a+b)
      val tot = vuetot.reduce((a,b)=> a+b)






      //5. Quelle est la moyenne des notes par acteur dans cet échantillon ?  ?
      //   Pour cette question, il faut utiliser les Pair-RDD

      val pair_RDD = film.map(elem => (elem.split(";")(3).toString, (1.0, elem.split(";")(2).toDouble)) )
      val countSums1 = pair_RDD.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))

      val keyMeans1 = countSums1.mapValues(avgCount1 => avgCount1._2 / avgCount1._1)
      keyMeans1.foreach(println)



      //6.  Quelle est la moyenne des vues par acteur dans cet échantillon ?

      val pairRDD2 = film.map(elem => (elem.split(";")(1).toString, (1.0, elem.split(";")(1).toDouble)) )
      pairRDD2.foreach(println)
      //val withValue2 = pairRDD2.mapValues(e => (1.0, e))
      val countSums2 = pairRDD2.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2 ))
      val keyMeans2 = countSums2.mapValues(avgCount2 => avgCount2._2 / avgCount2._1)
      keyMeans2.foreach(println)





      // ######### EXERCICE 2 ##########


      // 1. Lire le fichier "films.csv" avec la commande suivante :
      val df: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("data/donnees.csv")
      //df.show

      // 2. Nommez les colonnes comme suit : nom_film, nombre_vues, note_film, acteur_principal

      val df1 = df.toDF("nom_film", "nombre_vues", "note_film", "acteur_principal")
      df1.show


      // 3.Refaire les questions 2, 3, 4 et 5 en utilisant les DataFrames.



      //Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
      val q2= df1.filter( col("acteur_principal") ==="Di Caprio")
      print(q2.count())

      // Quelle est la moyenne des notes des films de Di Caprio ?
      val DCP=df1.filter(col("acteur_principal")==="Di Caprio")
      val q3 = DCP.groupBy("acteur_principal").mean("note_film")
      q3.show


      //Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?
      val film1=df1.groupBy(col("acteur_principal")).sum("nombre_vues")
      val filmDCP = film1.filter(col("acteur_principal") ==="Di Caprio")
      val total = film1.select(col("sum(nombre_vues)")).rdd.map(_(0).asInstanceOf[Long]).reduce(_+_)
      //println(total)
      val x = filmDCP.withColumn("pourcentage",(col("sum(nombre_vues)")/total)*100)
      x.show


      //5. Quelle est la moyenne des notes par acteur dans cet échantillon ?

      val q5 = df1.groupBy("acteur_principal").mean("note_film")
      q5.show




      // 4. Créer une nouvelle colonne dans ce DataFrame, "pourcentage de vues", contenant le pourcentage de vues pour chaque film (combien de fois le film a-t-il été vu par rapport aux vues globales ?)
      val pourcentage = film1.withColumn("pourcentage de vues ",(col("sum(nombre_vues)")/total)*100)
      pourcentage.show


  }

}

