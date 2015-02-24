package com.akarthik.lsh

import scala.io.Source
import scala.Option
import scala.Some
import scala.None

/** A Locality Sensitive Hash that hashes the documents into buckets
  *
  *@constructor : create a new instance.s
   @param : shingleLength. Default value  = 3, 
   @param : minHashLengh. The default value is = 100
   @param : numberBands. The Default valeu is 10.
   @param : processedDocuments - which is a Tuple of (document , documennt - index) The processed documents may normalize the document by having just 
   one space between words and converting all characters to lower case.
   @param : threshold The default value for threshold is 0.8. 
   
   The parameters numberBands, threshold may be may be set so that threshold is aprroximately equal to  (1/numberBands)^(1/rows per band).
 **/

class LSH(shingleLength: Int = 3,
  minHashLength: Int = 100,
  numberBands: Int=10,
  processedDocuments: IndexedSeq[(String, Int)],
  threshold: Double=0.8) {

  val randomHashFunctions = randomLinearHashFunction(minHashLength); // IndexSeq[(int, int)]

  val documentShingles: Map[Int, Set[String]] = processedDocuments.map { document =>
    val shingles = document._1.toList.sliding(shingleLength)
      .map(_.mkString).toSet
    (document._2, shingles)
  }.toMap // documentShingles is a Map -> key == document index e value == set of shingles of the document

  val shingleVocab = documentShingles.values.flatten.toSet.toIndexedSeq.zipWithIndex.toMap

  var mBands: IndexedSeq[Band] = null;

  // Create an IndexSeq[(int, int)] with random values from 0 to 1000
  private def randomLinearHashFunction(n: Int) = {
    val slope = scala.util.Random.shuffle(0 to 1000);
    val const = scala.util.Random.shuffle(0 to 1000);
    slope.zip(const).take(minHashLength);
  }


  def findCandidates(shingles: Set[String]) = {
    val minHash = getMinHash(shingles);

    val subArrays = partitionArray(minHash).zipWithIndex; // IndexSeq[(Array[Double], Int)] ->
                                                          // Array[Double] => part of the minHash
                                                          // Int => index of the part

    val candidates = subArrays.flatMap { subArray =>
      val index = subArray._2;
      val hashedBucket = mBands(index).getCollisionObjects(subArray._1) // encontra a lista com os indices dos documentos que colidiram para essa band
      hashedBucket
    }.flatten.toSet // cada band retornara uma List com os indices dos documentos que colidiram, entao candites ate esse ponto eh IndexSeq[List[Int]]
    // transforma em mutavel e depois em um SET (pra remover os indices repetidos acredito eu), entao no final tem-se canidates = Set[Int] com os indices que colidiram

    candidates
  }

  /** Returns documents that have Jaccard Similarity greater than threshold Assumes that a documents
   have already been hashed
  @tparam : document . The document for which similar documents have to be identified
    **/

  def findSimilar(document: String) = {
    val shingles = document.toList.sliding(shingleLength)
      .map(_.mkString)
      .map(shingle => shingle.toLowerCase)
      .toSet;

    val candidates = findCandidates(shingles);
    candidates.filter(candidate => JaccardSimilarity(shingles, documentShingles(candidate)) > threshold) // vai comparar o shingle do documento 'atual' (o qual busca-se a similaridade),
    // com o shingle de cada documento que foi dito como candidato. Aqueles que forem maior que o theshold sao considerados documentos similares
  }

  /** Returns the Min Hash of a document
    *@tparam : The shingle representation for that document
    **/

  def getMinHash(shingles: Set[String]) = {

    val minHash = Array.fill[Double](minHashLength)(Double.PositiveInfinity); // inicializa um Array de doubles de tamanho 'minHashLength' com Infinitys

    shingles.filter(x => shingleVocab.contains(x)) // filtra apenas os shingles que estao no 'shingleVocab'
      .foreach { shingle =>                        // pra cada shingle
      val shingleIndex = shingleVocab(shingle);    // pega o indice dele no 'shingleVocab'
    var hashIndex = 0;
      randomHashFunctions.foreach { function =>  // pra cada hash function
        val permutedIndex = (function._1 * shingleIndex + function._2) % shingleVocab.size // aplica o Hash => (valor1_hash + indice + valor2_hash) / tamanho do 'shingleVocab'

        if (minHash(hashIndex) > permutedIndex) // se o valor para o indice atual for maior do que o 'permutedIndex' esse valor eh substituido
          minHash(hashIndex) = permutedIndex    // com isso o valores de 'minHash' que eram 'Infinity' vao sendo atualizados

        hashIndex += 1; // o indice atual da 'minHash' eh atualizado
      }

    }

    minHash

  }

  /** Partition the min-hash into  numberBands bands
  @tparam : The shingle represenatation of the document
    **/

  def partitionArray(minHash: Array[Double]): IndexedSeq[Array[Double]] = {

    if (minHash.length < numberBands) {
      println("number of bands exceeds minHash")
      System.exit(0);
    }

    val elementsPerBand = (minHash.length / numberBands);
    (0 to numberBands - 1).map { bandIndex =>
      val start = bandIndex * elementsPerBand
      val end = start + elementsPerBand;
      minHash.slice(start, end);
    } // cria um IndexedSeq[Array[Double]] onde cada elemento eh um pedaco do minHash, o tamanho desse pedaco eh definido por 'elementsPerBand'
    // o tamanho do 'IndexedSeq' sera igual ao numero de bands
  }

  /** Creates a locality sensitive hash for the all the processed documents **/

  def createHash() = {

    val minHashCollection = documentShingles.mapValues(shingleSet => getMinHash(shingleSet)) // um Map[Int, Array[Double]], onde chave == indice do documento e valor == minHash
    val bands =

      (0 to numberBands - 1).map { bandIndex =>
        val elementsPerBand = (1.0 * minHashLength / numberBands).ceil.toInt
        val start = bandIndex * elementsPerBand
        val end = if (bandIndex == numberBands - 1) minHashLength else start + elementsPerBand; // se o 'bandIndex' for igual ao limite o 'end' recebe o tamanho do array
      val subArray = minHashCollection.map(document => (document._1, document._2.slice(start, end))) // Map[Int, Array[Double]] ->
                                                                                                    // Int => indice do documento,
                                                                                                    // Array[Double] => pedaco do min hash
                                                                                                    // cada linha desse Map representa um documento
      val band = new Band()
        subArray.foreach(array => band.hash(array)) // mapeia o pedaco do min hash pro bucket (ver comentarios do metodo 'hash')
        band // cada band representa um pedaco do minHash. Ex: band1 = band referente a primeira parte do minHash para todos os documentos
            // cada band tem seus buckets, entao os buckets da band1 sao referentes a primeira parte do minHash de cada documento
      }

    mBands = bands
  }

}

/** Represents one band of the Locality Sensitive Hash **/

class Band() {
  import scala.collection.mutable.ArrayBuffer

  val buckets = scala.collection.mutable.Map[List[Double], ArrayBuffer[Int]]() // chave == pedaco do minHash e valor == indice do documento

  /** Hashes the sub- array into buckets **/
  def hash(subArray: (Int, Array[Double])) {
    buckets.get(subArray._2.toList) match { // pega o bucket correspondente ao pedaco do minHash
      case Some(value: ArrayBuffer[Int]) => value += subArray._1; //caso esse bucket exista, o indice do documento eh adicionado no array referente a tal bucket
      case None => buckets(subArray._2.toList) = ArrayBuffer(subArray._1) //caso nao exista esse bucket ainda ele eh criado e o indice do documento eh adicionado ao seu array
    }

  }

  /** Returns the documents that collide to the same bucket **/
  def getCollisionObjects(subArray: Array[Double]): Option[List[Int]] = {
    buckets.get(subArray.toList) match {
      case Some(value: ArrayBuffer[Int]) => Some(value.toList); //caso o bucket correspondente ao 'subArray' exista, a lista dos indices dos documentos eh retornada
      case None => buckets(subArray.toList) = ArrayBuffer(-1); None //caso contrario, cria-se tal bucket e adiciona -1 ao seu array (Pq isso??)
    }
  }

}

/** Computes the Jaccrd Similarity of two sets**/

object JaccardSimilarity {
  def apply(set1: Set[String], set2: Set[String]): Double = {
    val intersection = set1.intersect(set2).size
    val union = set2.union(set2).size

    return (intersection * 1.0) / union
  }
}
