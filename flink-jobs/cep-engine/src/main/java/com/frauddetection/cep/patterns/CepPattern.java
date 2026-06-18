package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;

public interface CepPattern {

    String getName();

    boolean matches(Transaction tx) throws Exception;
}
