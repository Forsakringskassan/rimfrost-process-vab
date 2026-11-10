package se.fk.github.rimfrost.vardavboskap;

import org.junit.jupiter.api.Test;
import se.fk.rimfrost.framework.process.helpers.ProcessbeskrivningVerifier;

class VabProcessbeskrivningTest
{
   @Test
   void processbeskrivning_should_contain_all_aktiviteter()
   {
      ProcessbeskrivningVerifier.assertAllAktiviteterInProcessbeskrivning("vab.bpmn");
   }
}
